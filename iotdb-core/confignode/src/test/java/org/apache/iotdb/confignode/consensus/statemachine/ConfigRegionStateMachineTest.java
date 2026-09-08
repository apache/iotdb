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

package org.apache.iotdb.confignode.consensus.statemachine;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.TestOnlyPlan;
import org.apache.iotdb.confignode.persistence.executor.ConfigPlanExecutor;
import org.apache.iotdb.confignode.writelog.io.SingleFileLogReader;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.utils.writelog.LogWriter;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class ConfigRegionStateMachineTest {

  @Test
  public void testParseStartIndex() {
    Assert.assertEquals(1, ConfigRegionStateMachine.parseStartIndex("log_1_10"));
    Assert.assertEquals(11, ConfigRegionStateMachine.parseStartIndex("log_11_20"));
    Assert.assertEquals(21, ConfigRegionStateMachine.parseStartIndex("log_inprogress_21"));
    Assert.assertEquals(0, ConfigRegionStateMachine.parseStartIndex("invalid"));
  }

  @Test
  public void testParseEndIndex() {
    Assert.assertEquals(10, ConfigRegionStateMachine.parseEndIndex("log_1_10"));
    Assert.assertEquals(20, ConfigRegionStateMachine.parseEndIndex("log_11_20"));
    Assert.assertEquals(21, ConfigRegionStateMachine.parseEndIndex("log_inprogress_21"));
    Assert.assertEquals(0, ConfigRegionStateMachine.parseEndIndex("invalid"));
  }

  @Test
  public void testLogFilesSortByEndIndex() {
    List<String> filenames =
        new ArrayList<>(Arrays.asList("log_inprogress_21", "log_11_20", "log_1_10"));

    filenames.sort(Comparator.comparingLong(ConfigRegionStateMachine::parseEndIndex));

    Assert.assertEquals(Arrays.asList("log_1_10", "log_11_20", "log_inprogress_21"), filenames);
  }

  @Test
  public void testFailedSimpleConsensusWriteRollsBackPersistedPlan() throws Exception {
    ConfigNodeConfig conf = ConfigNodeDescriptor.getInstance().getConf();
    String originalConsensusProtocol = conf.getConfigNodeConsensusProtocolClass();
    Path tempLogFile = Files.createTempFile("confignode-simple-consensus", ".wal");
    ConfigPlanExecutor executor = Mockito.mock(ConfigPlanExecutor.class);
    Mockito.when(executor.executeNonQueryPlan(Mockito.any(ConfigPhysicalPlan.class)))
        .thenReturn(new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));
    ConfigRegionStateMachine stateMachine = new ConfigRegionStateMachine(null, executor);

    try {
      conf.setConfigNodeConsensusProtocolClass(ConsensusFactory.SIMPLE_CONSENSUS);
      setField(stateMachine, "simpleLogFile", tempLogFile.toFile());
      setField(stateMachine, "simpleLogWriter", new LogWriter(tempLogFile.toFile(), false));
      setField(stateMachine, "endIndex", 0);

      TSStatus status = stateMachine.write(new TestOnlyPlan());

      Assert.assertEquals(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(), status.getCode());
      Assert.assertEquals(0, Files.size(tempLogFile));
      Assert.assertEquals(0, getField(stateMachine, "endIndex"));
      closeSimpleLogWriter(stateMachine);
      assertNoReplayablePlans(tempLogFile);
    } finally {
      closeSimpleLogWriter(stateMachine);
      Files.deleteIfExists(tempLogFile);
      conf.setConfigNodeConsensusProtocolClass(originalConsensusProtocol);
    }
  }

  private static void setField(Object target, String fieldName, Object value) throws Exception {
    Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }

  private static Object getField(Object target, String fieldName) throws Exception {
    Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    return field.get(target);
  }

  private static void assertNoReplayablePlans(Path tempLogFile) throws Exception {
    SingleFileLogReader logReader = new SingleFileLogReader(tempLogFile.toFile());
    try {
      Assert.assertFalse(logReader.hasNext());
    } finally {
      logReader.close();
    }
  }

  private static void closeSimpleLogWriter(ConfigRegionStateMachine stateMachine) throws Exception {
    LogWriter logWriter = (LogWriter) getField(stateMachine, "simpleLogWriter");
    if (logWriter != null) {
      logWriter.close();
    }
  }
}
