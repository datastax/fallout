/*
 * Copyright 2021 DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.fallout.components.common.provider;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedOutputStream;
import java.nio.file.Path;
import java.util.Map;
import java.util.Vector;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.IdentityRepository;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.agentproxy.RemoteIdentityRepository;
import com.jcraft.jsch.agentproxy.connector.SSHAgentConnector;
import com.jcraft.jsch.agentproxy.usocket.JNAUSocketFactory;

import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.Utils;
import com.datastax.fallout.ops.commands.NodeResponse;
import com.datastax.fallout.util.Exceptions;

import static com.datastax.fallout.components.common.provisioner.AbstractSshProvisioner.connectToChannelWithRetries;

public class SshProvider extends ShellProvider
{
    public static final int SSH_CONNECT_TIMEOUT = 60 * 1000;

    private static boolean sshAgentIsAvailable()
    {
        return System.getenv("SSH_AUTH_SOCK") != null;
    }

    public enum SshShell
    {
        BASH,
        POWERSHELL
    }

    private final String username;
    private final String host;
    private final Integer port;
    private final Path privateKey;
    private final String password;
    private final SshShell defaultSshShell;

    private Session session;

    public SshProvider(Node node, String username, String host, Integer port, Path privateKey, String password,
        SshShell defaultSshShell)
    {
        super(node);
        this.username = username;
        this.host = host;
        this.port = port;
        this.privateKey = privateKey;
        this.password = password;
        this.defaultSshShell = defaultSshShell;
    }

    @Override
    public String name()
    {
        return "ssh";
    }

    static class SshNodeResponse extends NodeResponse
    {
        final ChannelExec channel;
        final InputStream outStream;
        final InputStream errStream;

        SshNodeResponse(Node owner, String command, ChannelExec channel) throws IOException
        {
            super(owner, command);

            this.channel = channel;
            this.outStream = channel.getInputStream();
            this.errStream = channel.getErrStream();
        }

        @Override
        public InputStream getOutputStream() throws IOException
        {
            return outStream;
        }

        @Override
        public InputStream getErrorStream() throws IOException
        {
            return errStream;
        }

        @Override
        public int getExitCode()
        {
            return channel.getExitStatus();
        }

        @Override
        public boolean isCompleted()
        {
            return channel.isClosed();
        }

        @Override
        public void doKill()
        {
            channel.disconnect();
        }
    }

    @Override
    public NodeResponse execute(String command)
    {
        String sshCommand;
        switch (defaultSshShell)
        {
            case POWERSHELL:
                sshCommand = command;
                break;
            case BASH:
            default:
                sshCommand = String.join(" ", Utils.wrapCommandWithBash(command, true)).trim();
                break;
        }

        try
        {
            ChannelExec channel = (ChannelExec) getConnectedSession().openChannel("exec");

            PipedOutputStream out = new PipedOutputStream();
            PipedOutputStream err = new PipedOutputStream();

            if (sshAgentIsAvailable())
            {
                channel.setAgentForwarding(true);
            }
            channel.setInputStream(null);
            channel.setOutputStream(out);
            channel.setErrStream(err);
            channel.setCommand(sshCommand);

            //Must fetch the inputStreams before connecting.
            NodeResponse response = new SshNodeResponse(node(), command, channel);

            connectToChannelWithRetries(channel, logger());

            return response;
        }
        catch (JSchException | IOException e)
        {
            throw new RuntimeException("Failed to execute ssh cmd: " + sshCommand, e);
        }
    }

    @Override
    public void unregister()
    {
        if (session != null)
        {
            session.disconnect();
        }
        super.unregister();
    }

    public synchronized Session getConnectedSession()
    {
        if (session != null && session.isConnected())
        {
            return session;
        }
        logger().info("SSH Session disconnected, trying to re-connect");
        Session session = createSshSession();
        try
        {
            session.connect(SSH_CONNECT_TIMEOUT);
            node().logger().info("ssh connected for node " + node().getId());
        }
        catch (JSchException e)
        {
            node().logger().error(
                "Encountered JSchException. startSshSession was called with the following arguments: node={}, username={}, host={}, port={}, privateKey={}, password={})",
                node(), username, host, port, privateKey, password);
            throw new RuntimeException(e);
        }
        if (!session.isConnected())
        {
            throw new RuntimeException("Failed to create connected SSH session");
        }
        this.session = session;
        return session;
    }

    /** All changes are made to local, but results are returned from local and remote */
    private static class ImmutableRemoteIdentityRepository implements IdentityRepository
    {
        private final RemoteIdentityRepository remote;
        private final IdentityRepository local;

        private ImmutableRemoteIdentityRepository(IdentityRepository local, RemoteIdentityRepository remote)
        {
            this.remote = remote;
            this.local = local;
        }

        @Override
        public String getName()
        {
            return remote.getName();
        }

        @Override
        public int getStatus()
        {
            return remote.getStatus();
        }

        @Override
        public Vector getIdentities()
        {
            final var result = new Vector();
            result.addAll(remote.getIdentities());
            result.addAll(local.getIdentities());
            return result;
        }

        @Override
        public boolean add(byte[] bytes)
        {
            return local.add(bytes);
        }

        @Override
        public boolean remove(byte[] bytes)
        {
            return local.remove(bytes);
        }

        @Override
        public void removeAll()
        {
            local.removeAll();
        }
    }

    private Session createSshSession()
    {
        // these values will be applied in the constructor of the jsch Session
        JSch.setConfig("StrictHostKeyChecking", "no");
        // SSH has a TCPKeepAlive option, but JSCH doesn't seem to ever check it
        JSch.setConfig("TCPKeepAlive", "yes");

        try
        {
            JSch jSch = new JSch();
            if (sshAgentIsAvailable())
            {
                jSch.setIdentityRepository(
                    new ImmutableRemoteIdentityRepository(
                        jSch.getIdentityRepository(),
                        new RemoteIdentityRepository(
                            Exceptions.getUnchecked(() -> new SSHAgentConnector(new JNAUSocketFactory())))));
            }
            if (privateKey != null)
            {
                jSch.addIdentity(privateKey.toAbsolutePath().toString());
            }
            Session session = jSch.getSession(username, host, port);
            // These are necessary to avoid problems with latent connections being closed
            session.setServerAliveInterval(15 * 1000);
            session.setServerAliveCountMax(20);
            if (password != null)
            {
                session.setPassword(password);
            }
            return session;
        }
        catch (JSchException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, String> toInfoMap()
    {
        return Map.of("host", session.getHost(), "port", String.valueOf(session.getPort()));
    }
}
