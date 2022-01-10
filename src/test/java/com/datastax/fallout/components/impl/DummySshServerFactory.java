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
package com.datastax.fallout.components.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.sshd.common.Factory;
import org.apache.sshd.server.Command;
import org.apache.sshd.server.CommandFactory;
import org.apache.sshd.server.Environment;
import org.apache.sshd.server.ExitCallback;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.server.subsystem.sftp.SftpSubsystemFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.ops.WritablePropertyGroup;

public class DummySshServerFactory
{
    private static final Logger logger = LoggerFactory.getLogger(DummySshServerFactory.class);
    private static final String USERNAME = "unittest";
    private static final String PASSWORD = "123";
    private static final int PORT = 19292;

    public static SshServer createServer() throws IOException
    {
        return createServer(new DummyCommandFactory());
    }

    public static SshServer createServer(CommandFactory cmdFactory) throws IOException
    {
        SshServer sshd = SshServer.setUpDefaultServer();
        sshd.setPort(PORT);
        sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider());
        sshd.setCommandFactory(cmdFactory);
        sshd.setPasswordAuthenticator(
            (username, password, session) -> USERNAME.equals(username) && PASSWORD.equals(password));
        sshd.setSubsystemFactories(List.of(new SftpSubsystemFactory()));
        sshd.start();
        return sshd;
    }

    public static WritablePropertyGroup createProperties()
    {
        WritablePropertyGroup properties = new WritablePropertyGroup();
        properties.put("fallout.provisioner.sshonly.user.name", USERNAME);
        properties.put("fallout.system.user.privatekey", "none");
        properties.put("fallout.provisioner.sshonly.host", "localhost");
        properties.put("fallout.provisioner.sshonly.user.password", PASSWORD);
        properties.put("fallout.provisioner.sshonly.port", Integer.toString(PORT));
        properties.put("test.config.foo", "abc");
        return properties;
    }

    //Helpers for SSHServer
    //based on http://stackoverflow.com/questions/18694108/apache-mina-sshd-problems-with-authentication-method-when-connecting-to-server/21553897#21553897
    private static class MyCommand implements Command
    {
        private String name;

        private OutputStream outputStream;
        private OutputStream errorStream;
        private ExitCallback exitCallback;

        private static final Pattern EXIT_PATTERN = Pattern.compile(".*\\bexit (-?\\d+)\\b.*");

        MyCommand(String name)
        {
            this.name = name;
        }

        public void setInputStream(final InputStream inputStream)
        {
        }

        public void setOutputStream(final OutputStream outputStream)
        {
            this.outputStream = outputStream;
        }

        public void setErrorStream(OutputStream outputStream)
        {
            this.errorStream = outputStream;
        }

        public void setExitCallback(ExitCallback exitCallback)
        {
            this.exitCallback = exitCallback;
        }

        public void start(Environment environment) throws IOException
        {
            try
            {
                final PrintWriter writer = new PrintWriter(outputStream, true);
                writer.println("STDOUT 1");
                writer.println("STDOUT 2");
                writer.println("STDOUT 3");
                writer.flush();

                final PrintWriter errWriter = new PrintWriter(errorStream, true);
                errWriter.println("STDERR 1");
                errWriter.println("STDERR 2");
                errWriter.println("STDERR 3");
                errWriter.flush();

                if (name.contains("sudo"))
                {
                    // sudo makedirs
                    exitCallback.onExit(0);
                    return;
                }

                // execute supported unit test commands inline
                Matcher exitMatch = EXIT_PATTERN.matcher(name);
                if (exitMatch.matches())
                {
                    exitCallback.onExit(Integer.valueOf(exitMatch.group(1)));
                    return;
                }

                throw new RuntimeException("Unhandled command: " + name);
            }
            catch (final Exception e)
            {
                e.printStackTrace();
                exitCallback.onExit(666);
            }
        }

        public void destroy()
        {
            // client or server closed connection, clean up resources here
        }
    }

    public static class DummyCommandFactory implements CommandFactory, Factory<Command>
    {
        public Command createCommand(String s)
        {
            return new MyCommand(s);
        }

        public Command create()
        {
            return createCommand("exec");
        }
    }
}
