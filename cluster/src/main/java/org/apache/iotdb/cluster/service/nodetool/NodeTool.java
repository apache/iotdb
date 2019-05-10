/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.cluster.service.nodetool;

import static java.lang.String.format;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.airlift.airline.Cli;
import io.airlift.airline.Help;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;
import io.airlift.airline.ParseArgumentsMissingException;
import io.airlift.airline.ParseArgumentsUnexpectedException;
import io.airlift.airline.ParseCommandMissingException;
import io.airlift.airline.ParseCommandUnrecognizedException;
import io.airlift.airline.ParseOptionConversionException;
import io.airlift.airline.ParseOptionMissingException;
import io.airlift.airline.ParseOptionMissingValueException;
import java.io.IOException;
import java.util.List;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import org.apache.iotdb.cluster.service.ClusterMonitor;
import org.apache.iotdb.cluster.service.ClusterMonitorMBean;

public class NodeTool {

  public static void main(String... args) {
    List<Class<? extends Runnable>> commands = Lists.newArrayList(
        Help.class,
        Ring.class,
        StorageGroup.class,
        Host.class,
        Lag.class,
        Query.class
    );

    Cli.CliBuilder<Runnable> builder = Cli.builder("nodetool");

    builder.withDescription("Manage your IoTDB cluster")
        .withDefaultCommand(Help.class)
        .withCommands(commands);

    Cli<Runnable> parser = builder.build();

    int status = 0;
    try {
      Runnable parse = parser.parse(args);
      parse.run();
    } catch (IllegalArgumentException |
        IllegalStateException |
        ParseArgumentsMissingException |
        ParseArgumentsUnexpectedException |
        ParseOptionConversionException |
        ParseOptionMissingException |
        ParseOptionMissingValueException |
        ParseCommandMissingException |
        ParseCommandUnrecognizedException e) {
      badUse(e);
      status = 1;
    } catch (Exception e) {
      err(Throwables.getRootCause(e));
      status = 2;
    }

    System.exit(status);
  }

  private static void badUse(Exception e) {
    System.out.println("nodetool: " + e.getMessage());
    System.out.println("See 'nodetool help' or 'nodetool help <command>'.");
  }

  private static void err(Throwable e) {
    System.err.println("error: " + e.getMessage());
    System.err.println("-- StackTrace --");
    System.err.println(Throwables.getStackTraceAsString(e));
  }

  public abstract static class NodeToolCmd implements Runnable {

    @Option(type = OptionType.GLOBAL, name = {"-h",
        "--host"}, description = "Node hostname or ip address")
    private String host = "127.0.0.1";

    @Option(type = OptionType.GLOBAL, name = {"-p",
        "--port"}, description = "Remote jmx agent port number")
    private String port = "31999";

    private static final String JMX_URL_FORMAT = "service:jmx:rmi:///jndi/rmi://%s:%s/jmxrmi";

    @Override
    public void run() {
      try {
        MBeanServerConnection mbsc = connect();
        ObjectName name = new ObjectName(ClusterMonitor.MBEAN_NAME);
        ClusterMonitorMBean clusterMonitorProxy = JMX
            .newMBeanProxy(mbsc, name, ClusterMonitorMBean.class);
        execute(clusterMonitorProxy);
      } catch (MalformedObjectNameException e) {
        e.printStackTrace();
      }
    }

    protected abstract void execute(ClusterMonitorMBean probe);

    private MBeanServerConnection connect() {
      MBeanServerConnection mbsc = null;

      try {
        String jmxURL = String.format(JMX_URL_FORMAT, host, port);
        JMXServiceURL serviceURL = new JMXServiceURL(jmxURL);
        JMXConnector connector = JMXConnectorFactory.connect(serviceURL);
        mbsc = connector.getMBeanServerConnection();
      } catch (IOException e) {
        Throwable rootCause = Throwables.getRootCause(e);
        System.err.println(format("nodetool: Failed to connect to '%s:%s' - %s: '%s'.", host, port,
            rootCause.getClass().getSimpleName(), rootCause.getMessage()));
        System.exit(1);
      }

      return mbsc;
    }
  }
}
