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

import org.apache.iotdb.db.conf.IoTDBConstant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import java.lang.management.ManagementFactory;

public class JMXService implements IService {

  private static final Logger logger = LoggerFactory.getLogger(JMXService.class);

  private JMXService() {}

  public static JMXService getInstance() {
    return JMXServerHolder.INSTANCE;
  }

  /** function for registering MBean. */
  public static void registerMBean(Object mbean, String name) {
    try {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName objectName = new ObjectName(name);
      if (!mbs.isRegistered(objectName)) {
        mbs.registerMBean(mbean, objectName);
      }
    } catch (MalformedObjectNameException
        | InstanceAlreadyExistsException
        | MBeanRegistrationException
        | NotCompliantMBeanException e) {
      logger.error("Failed to registerMBean {}", name, e);
    }
  }

  /** function for deregistering MBean. */
  public static void deregisterMBean(String name) {
    try {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName objectName = new ObjectName(name);
      if (mbs.isRegistered(objectName)) {
        mbs.unregisterMBean(objectName);
      }
    } catch (MalformedObjectNameException
        | MBeanRegistrationException
        | InstanceNotFoundException e) {
      logger.error("Failed to unregisterMBean {}", name, e);
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.JMX_SERVICE;
  }

  @Override
  public void start() {
    String jmxPort = System.getProperty(IoTDBConstant.IOTDB_JMX_PORT);
    if (jmxPort == null) {
      logger.debug("{} JMX port is undefined", this.getID().getName());
    }
  }

  @Override
  public void stop() {
    // do nothing.
  }

  private static class JMXServerHolder {

    private static final JMXService INSTANCE = new JMXService();

    private JMXServerHolder() {}
  }
}
