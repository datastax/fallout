/*
 * Copyright 2020 DataStax, Inc.
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
package com.datastax.fallout.ops;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Lifted from the C* source code.
 */
public class JMXConnection
{
    private static final String FMT_URL = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";
    private final String host, username, password;
    private final int port;
    private JMXConnector jmxc;
    private MBeanServerConnection mbeanServerConn;

    public JMXConnection(String host, int port, String username, String password) throws IOException
    {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        connect();
    }

    private void connect() throws IOException
    {
        JMXServiceURL jmxUrl = new JMXServiceURL(String.format(FMT_URL, host, port));
        Map<String, Object> env = new HashMap<>();

        if (username != null)
            env.put(JMXConnector.CREDENTIALS, new String[] {username, password});

        jmxc = JMXConnectorFactory.connect(jmxUrl, env);
        mbeanServerConn = jmxc.getMBeanServerConnection();
    }

    public void close() throws IOException
    {
        jmxc.close();
    }

    public MBeanServerConnection getMbeanServerConn()
    {
        return mbeanServerConn;
    }

}
