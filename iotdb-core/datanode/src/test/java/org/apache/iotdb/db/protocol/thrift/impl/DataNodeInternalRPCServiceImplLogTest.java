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

package org.apache.iotdb.db.protocol.thrift.impl;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class DataNodeInternalRPCServiceImplLogTest {

  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();
  private static final IoTDBConfig DATANODE_CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static final String DISK_FULL_WARNING_MESSAGE_PREFIX =
      "The available disk space is : {}, ";

  private double previousDiskSpaceWarningThreshold;
  private NodeStatus previousNodeStatus;
  private String previousStatusReason;
  private int previousDataNodeId;

  @Before
  public void setUp() {
    previousDiskSpaceWarningThreshold = COMMON_CONFIG.getDiskSpaceWarningThreshold();
    previousNodeStatus = COMMON_CONFIG.getNodeStatus();
    previousStatusReason = COMMON_CONFIG.getStatusReason();
    previousDataNodeId = DATANODE_CONFIG.getDataNodeId();

    DATANODE_CONFIG.setDataNodeId(0);
    COMMON_CONFIG.setDiskSpaceWarningThreshold(0.5);
    COMMON_CONFIG.setNodeStatus(NodeStatus.Running);
    COMMON_CONFIG.setStatusReason(null);
    DataNodeInternalRPCServiceImpl.resetDiskFullWarningLogTime();
  }

  @After
  public void tearDown() {
    COMMON_CONFIG.setDiskSpaceWarningThreshold(previousDiskSpaceWarningThreshold);
    COMMON_CONFIG.setNodeStatus(previousNodeStatus);
    COMMON_CONFIG.setStatusReason(previousStatusReason);
    DATANODE_CONFIG.setDataNodeId(previousDataNodeId);
    DataNodeInternalRPCServiceImpl.resetDiskFullWarningLogTime();
  }

  @Test
  public void diskFullWarningLoggedOnlyOnceUntilDiskRecovers() {
    ch.qos.logback.classic.Logger logger =
        (ch.qos.logback.classic.Logger)
            LoggerFactory.getLogger(DataNodeInternalRPCServiceImpl.class);
    Level previousLevel = logger.getLevel();
    logger.setLevel(Level.WARN);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.setContext(logger.getLoggerContext());
    appender.start();
    logger.addAppender(appender);

    try {
      DataNodeInternalRPCServiceImpl.updateDiskStatusAndMaybeLog(COMMON_CONFIG, 10.0, 100.0, 0.1);
      DataNodeInternalRPCServiceImpl.updateDiskStatusAndMaybeLog(COMMON_CONFIG, 10.0, 100.0, 0.1);

      Assert.assertEquals(1, countLogEvents(appender, DISK_FULL_WARNING_MESSAGE_PREFIX));
      Assert.assertEquals(NodeStatus.ReadOnly, COMMON_CONFIG.getNodeStatus());
      Assert.assertEquals(NodeStatus.DISK_FULL, COMMON_CONFIG.getStatusReason());

      DataNodeInternalRPCServiceImpl.updateDiskStatusAndMaybeLog(COMMON_CONFIG, 90.0, 100.0, 0.9);
      Assert.assertEquals(NodeStatus.Running, COMMON_CONFIG.getNodeStatus());
      Assert.assertNull(COMMON_CONFIG.getStatusReason());

      DataNodeInternalRPCServiceImpl.updateDiskStatusAndMaybeLog(COMMON_CONFIG, 10.0, 100.0, 0.1);
      Assert.assertEquals(2, countLogEvents(appender, DISK_FULL_WARNING_MESSAGE_PREFIX));
    } finally {
      logger.detachAppender(appender);
      logger.setLevel(previousLevel);
      appender.stop();
    }
  }

  private long countLogEvents(ListAppender<ILoggingEvent> appender, String messagePrefix) {
    return appender.list.stream()
        .filter(event -> event.getMessage().startsWith(messagePrefix))
        .count();
  }
}
