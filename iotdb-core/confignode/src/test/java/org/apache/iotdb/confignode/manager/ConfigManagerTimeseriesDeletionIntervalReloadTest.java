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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.common.rpc.thrift.TSetConfigurationReq;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.ConfigurationFileUtils;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeConstant;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConfigManagerTimeseriesDeletionIntervalReloadTest {
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private long originalIntervalMs;
  private String originalConfPath;
  private Map<String, String> originalAppliedProperties;
  private ConfigManager configManager;
  private ProcedureManager procedureManager;

  @Before
  public void setUp() throws Exception {
    originalIntervalMs = CONF.getTimeseriesDeletionExecutionIntervalMs();
    originalConfPath = System.getProperty(ConfigNodeConstant.CONFIGNODE_CONF);
    originalAppliedProperties = new HashMap<>(ConfigurationFileUtils.getAppliedProperties());
    final Path confDir = temporaryFolder.newFolder().toPath();
    Files.writeString(
        confDir.resolve(CommonConfig.SYSTEM_CONFIG_NAME), "cn_seed_config_node=127.0.0.1:10710\n");
    System.setProperty(ConfigNodeConstant.CONFIGNODE_CONF, confDir.toString());
    configManager = mock(ConfigManager.class);
    procedureManager = mock(ProcedureManager.class);
    when(configManager.getProcedureManager()).thenReturn(procedureManager);
    doCallRealMethod().when(configManager).setConfiguration(any());
  }

  @After
  public void tearDown() {
    CONF.setTimeseriesDeletionExecutionIntervalMs(originalIntervalMs);
    ConfigurationFileUtils.getAppliedProperties().clear();
    ConfigurationFileUtils.getAppliedProperties().putAll(originalAppliedProperties);
    if (originalConfPath == null) {
      System.clearProperty(ConfigNodeConstant.CONFIGNODE_CONF);
    } else {
      System.setProperty(ConfigNodeConstant.CONFIGNODE_CONF, originalConfPath);
    }
  }

  @Test
  public void testChangedIntervalUpdatesProcedureExecutor() {
    final long updatedIntervalMs = originalIntervalMs + 1;
    assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        configManager
            .setConfiguration(
                request(
                    Map.of("timeseries_deletion_execution_interval_ms", "" + updatedIntervalMs)))
            .getCode());
    assertEquals(updatedIntervalMs, CONF.getTimeseriesDeletionExecutionIntervalMs());
    verify(procedureManager).updateTimeseriesDeletionExecutionIntervalMs(updatedIntervalMs);
  }

  @Test
  public void testLaterValidationFailureStillUpdatesSchedulerAndSameValueRetry() {
    final long updatedIntervalMs = originalIntervalMs + 1;
    assertEquals(
        TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(),
        configManager
            .setConfiguration(
                request(
                    Map.of(
                        "timeseries_deletion_execution_interval_ms",
                        "" + updatedIntervalMs,
                        "pipe_all_sinks_rate_limit_bytes_per_second",
                        "invalid")))
            .getCode());
    assertEquals(updatedIntervalMs, CONF.getTimeseriesDeletionExecutionIntervalMs());
    verify(procedureManager).updateTimeseriesDeletionExecutionIntervalMs(updatedIntervalMs);

    assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        configManager
            .setConfiguration(
                request(
                    Map.of("timeseries_deletion_execution_interval_ms", "" + updatedIntervalMs)))
            .getCode());
    verify(procedureManager, times(2))
        .updateTimeseriesDeletionExecutionIntervalMs(updatedIntervalMs);
  }

  @Test
  public void testNonPositiveIntervalsAreRejected() {
    for (final String invalid : new String[] {"0", "-1"}) {
      assertEquals(
          TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(),
          configManager
              .setConfiguration(
                  request(Map.of("timeseries_deletion_execution_interval_ms", invalid)))
              .getCode());
      assertEquals(originalIntervalMs, CONF.getTimeseriesDeletionExecutionIntervalMs());
    }
  }

  private static TSetConfigurationReq request(final Map<String, String> configs) {
    return new TSetConfigurationReq().setNodeId(CONF.getConfigNodeId()).setConfigs(configs);
  }
}
