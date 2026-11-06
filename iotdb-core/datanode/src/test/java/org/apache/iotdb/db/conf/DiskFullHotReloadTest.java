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
package org.apache.iotdb.db.conf;

import org.apache.iotdb.common.rpc.thrift.TSetConfigurationReq;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class DiskFullHotReloadTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final IoTDBDescriptor descriptor = IoTDBDescriptor.getInstance();
  private final CommonConfig config = CommonDescriptor.getInstance().getConfig();
  private final String originalConf = System.getProperty(IoTDBConstant.IOTDB_CONF);
  private final NodeStatus originalStatus = config.getNodeStatus();
  private final String originalReason = config.getStatusReason();
  private final boolean originalPartialInsert = descriptor.getConfig().isEnablePartialInsert();
  private Path configFile;

  @Before
  public void setUp() throws IOException {
    configFile = temporaryFolder.newFile(CommonConfig.SYSTEM_CONFIG_NAME).toPath();
    System.setProperty(IoTDBConstant.IOTDB_CONF, configFile.getParent().toString());
    config.setNodeStatus(NodeStatus.Running);
  }

  @After
  public void tearDown() {
    if (originalConf == null) {
      System.clearProperty(IoTDBConstant.IOTDB_CONF);
    } else {
      System.setProperty(IoTDBConstant.IOTDB_CONF, originalConf);
    }
    descriptor.getConfig().setEnablePartialInsert(originalPartialInsert);
    config.setNodeStatus(NodeStatus.Running);
    config.setNodeStatusWithReason(originalStatus, originalReason);
  }

  @Test
  public void loadSameConfigurationClearsOldDiskFullChecks() throws Exception {
    config.setReadOnlyForDiskFull("old directory", () -> false);
    descriptor.loadHotModifiedProps();
    assertRecoveredAndCleared();
  }

  @Test
  public void setUnrelatedPropertyClearsOldDiskFullChecks() {
    config.setReadOnlyForDiskFull("old directory", () -> false);
    Map<String, String> update =
        Map.of("enable_partial_insert", Boolean.toString(!originalPartialInsert));
    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), setConfiguration(update));
    assertEquals(!originalPartialInsert, descriptor.getConfig().isEnablePartialInsert());
    assertRecoveredAndCleared();
  }

  @Test
  public void unsuccessfulAndEmptyReloadsKeepDiskFull() throws IOException {
    config.setReadOnlyForDiskFull("old directory", () -> false);
    assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), setConfiguration(Collections.emptyMap()));
    assertReadOnly(NodeStatus.DISK_FULL);

    // The regular properties have loaded when the metrics phase rejects this value.
    Files.writeString(configFile, "dn_metric_level=INVALID\n");
    assertThrows(IllegalArgumentException.class, descriptor::loadHotModifiedProps);
    assertReadOnly(NodeStatus.DISK_FULL);

    // SET can apply properties in memory and still return an error if the file is missing.
    Files.delete(configFile);
    assertEquals(
        TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(),
        setConfiguration(Map.of("enable_partial_insert", Boolean.toString(originalPartialInsert))));
    assertReadOnly(NodeStatus.DISK_FULL);
  }

  @Test
  public void successfulReloadsKeepOtherReadOnlyReasons() throws Exception {
    config.setNodeStatusWithReason(NodeStatus.ReadOnly, NodeStatus.MANUAL);
    descriptor.loadHotModifiedProps();
    assertReadOnly(NodeStatus.MANUAL);

    config.setNodeStatus(NodeStatus.Running);
    String reason = NodeStatus.UNRECOVERABLE_ERROR + ", I/O error";
    config.setNodeStatusWithReason(NodeStatus.ReadOnly, reason);
    assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        setConfiguration(Map.of("enable_partial_insert", Boolean.toString(originalPartialInsert))));
    assertReadOnly(reason);
  }

  private int setConfiguration(Map<String, String> items) {
    return StorageEngine.getInstance()
        .setConfiguration(new TSetConfigurationReq(items, 0))
        .getCode();
  }

  private void assertRecoveredAndCleared() {
    assertEquals(NodeStatus.Running, config.getNodeStatus());
    assertNull(config.getStatusReason());
    // A later aggregate-only failure must have no callback left from the previous failure.
    config.setNodeStatusWithReason(NodeStatus.ReadOnly, NodeStatus.DISK_FULL);
    config.tryRecoverFromDiskFull();
    assertEquals(NodeStatus.Running, config.getNodeStatus());
    assertNull(config.getStatusReason());
  }

  private void assertReadOnly(String reason) {
    assertEquals(NodeStatus.ReadOnly, config.getNodeStatus());
    assertEquals(reason, config.getStatusReason());
  }
}
