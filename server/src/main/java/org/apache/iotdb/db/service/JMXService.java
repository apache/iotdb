/*
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
package org.apache.iotdb.db.service;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.StartupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JMXService implements IService {

  private static final Logger logger = LoggerFactory.getLogger(JMXService.class);

  private JMXConnectorServer jmxConnectorServer;

  private JMXService() {
  }

  public static final JMXService getInstance() {
    return JMXServerHolder.INSTANCE;
  }

  /**
   * function for registering MBean.
   */
  public static void registerMBean(Object mbean, String name) {
    try {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName objectName = new ObjectName(name);
      if (!mbs.isRegistered(objectName)) {
        mbs.registerMBean(mbean, objectName);
      }
    } catch (MalformedObjectNameException | InstanceAlreadyExistsException
        | MBeanRegistrationException
        | NotCompliantMBeanException e) {
      logger.error("Failed to registerMBean {}", name, e);
    }
  }

  /**
   * function for deregistering MBean.
   */
  public static void deregisterMBean(String name) {
    try {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName objectName = new ObjectName(name);
      if (mbs.isRegistered(objectName)) {
        mbs.unregisterMBean(objectName);
      }
    } catch (MalformedObjectNameException | MBeanRegistrationException
        | InstanceNotFoundException e) {
      logger.error("Failed to unregisterMBean {}", name, e);
    }
  }

  private JMXConnectorServer createJMXServer(boolean local) throws IOException {
    Map<String, Object> env = new HashMap<>();

    InetAddress serverAddress;
    if (local) {
      serverAddress = InetAddress.getLoopbackAddress();
      System.setProperty(IoTDBConstant.RMI_SERVER_HOST_NAME, serverAddress.getHostAddress());
    }
    int rmiPort = Integer.getInteger(IoTDBConstant.JMX_REMOTE_RMI_PORT, 0);

    return JMXConnectorServerFactory.newJMXConnectorServer(
        new JMXServiceURL("rmi", null, rmiPort), env, ManagementFactory.getPlatformMBeanServer());
  }

  @Override
  public ServiceType getID() {
    return ServiceType.JMX_SERVICE;
  }

  @Override
  public void start() throws StartupException {
    System.setProperty(IoTDBConstant.SERVER_RMI_ID, "true");
    boolean localOnly = false;
    String jmxPort = System.getProperty(IoTDBConstant.IOTDB_REMOTE_JMX_PORT_NAME);

    if (jmxPort == null) {
      localOnly = true;
      jmxPort = System.getProperty(IoTDBConstant.IOTDB_LOCAL_JMX_PORT_NAME, "31999");
    }

    if (jmxPort == null) {
      logger.warn("Failed to start {} because JMX port is undefined", this.getID().getName());
      return;
    }
    try {
      jmxConnectorServer = createJMXServer(localOnly);
      if (jmxConnectorServer == null) {
        return;
      }
      jmxConnectorServer.start();
      logger
          .info("{}: start {} successfully.", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
    } catch (IOException e) {
      throw new StartupException(this.getID().getName(), e.getMessage());
    }
  }

  @Override
  public void stop() {
    if (jmxConnectorServer != null) {
      try {
        jmxConnectorServer.stop();
        logger.info("{}: close {} successfully", IoTDBConstant.GLOBAL_DB_NAME,
            this.getID().getName());
      } catch (IOException e) {
        logger.error("Failed to stop {} because of: ", this.getID().getName(), e);
      }
    }
  }

  private static class JMXServerHolder {

    private static final JMXService INSTANCE = new JMXService();

    private JMXServerHolder() {
    }
  }
}
