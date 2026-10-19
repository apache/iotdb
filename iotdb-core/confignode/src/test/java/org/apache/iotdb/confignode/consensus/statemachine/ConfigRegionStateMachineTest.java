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
import org.apache.iotdb.confignode.consensus.request.write.confignode.UpdateVersionInfoPlan;
import org.apache.iotdb.confignode.i18n.ConfigNodeMessages;
import org.apache.iotdb.confignode.persistence.executor.ConfigPlanExecutor;
import org.apache.iotdb.confignode.rpc.thrift.TNodeVersionInfo;
import org.apache.iotdb.confignode.writelog.io.SingleFileLogReader;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.utils.writelog.LogWriter;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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

  @Test
  public void testForceFailureRollsBackAppendedPlan() throws Exception {
    assertPersistenceFailureRollsBack(true);
  }

  @Test
  public void testPartialAppendFailureRollsBackPlan() throws Exception {
    assertPersistenceFailureRollsBack(false);
  }

  private void assertPersistenceFailureRollsBack(boolean failOnForce) throws Exception {
    ConfigNodeConfig conf = ConfigNodeDescriptor.getInstance().getConf();
    String originalConsensusProtocol = conf.getConfigNodeConsensusProtocolClass();
    Path logFile = Files.createTempFile("confignode-persist-failure", ".wal");
    ConfigPlanExecutor executor = Mockito.mock(ConfigPlanExecutor.class);
    Mockito.when(executor.executeNonQueryPlan(Mockito.any(ConfigPhysicalPlan.class)))
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    ConfigRegionStateMachine stateMachine = new ConfigRegionStateMachine(null, executor);
    LogWriter writer = Mockito.spy(new LogWriter(logFile.toFile(), false));
    ConfigPhysicalPlan firstPlan = versionPlan(1);
    ConfigPhysicalPlan failedPlan = versionPlan(2);
    ConfigPhysicalPlan retryPlan = versionPlan(3);
    try {
      conf.setConfigNodeConsensusProtocolClass(ConsensusFactory.SIMPLE_CONSENSUS);
      setField(stateMachine, "simpleLogFile", logFile.toFile());
      setField(stateMachine, "simpleLogWriter", writer);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), stateMachine.write(firstPlan).getCode());
      long sizeBeforeWrite = Files.size(logFile);
      if (failOnForce) {
        Mockito.doThrow(new IOException("injected force failure")).when(writer).force();
      } else {
        Mockito.doAnswer(
                invocation -> {
                  Files.write(logFile, new byte[] {0, 0, 0}, StandardOpenOption.APPEND);
                  throw new IOException("injected partial append failure");
                })
            .when(writer)
            .write(Mockito.any(ByteBuffer.class));
      }

      TSStatus status = stateMachine.write(failedPlan);

      Assert.assertEquals(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(), status.getCode());
      Mockito.verify(executor, Mockito.never()).executeNonQueryPlan(failedPlan);
      Assert.assertEquals(sizeBeforeWrite, Files.size(logFile));
      Assert.assertEquals(1, getField(stateMachine, "endIndex"));
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), stateMachine.write(retryPlan).getCode());
      Assert.assertEquals(2, getField(stateMachine, "endIndex"));
      closeSimpleLogWriter(stateMachine);
      assertReplayablePlans(logFile, firstPlan, retryPlan);
    } finally {
      closeSimpleLogWriter(stateMachine);
      Files.deleteIfExists(logFile);
      conf.setConfigNodeConsensusProtocolClass(originalConsensusProtocol);
    }
  }

  @Test
  public void testCleanupFailureDisablesFurtherWrites() throws Exception {
    ConfigNodeConfig conf = ConfigNodeDescriptor.getInstance().getConf();
    String originalConsensusProtocol = conf.getConfigNodeConsensusProtocolClass();
    Path logFile = Files.createTempFile("confignode-cleanup-failure", ".wal");
    ConfigPlanExecutor executor = Mockito.mock(ConfigPlanExecutor.class);
    ConfigRegionStateMachine stateMachine = new ConfigRegionStateMachine(null, executor);
    LogWriter writer = Mockito.spy(new LogWriter(logFile.toFile(), false));
    try {
      conf.setConfigNodeConsensusProtocolClass(ConsensusFactory.SIMPLE_CONSENSUS);
      setField(stateMachine, "simpleLogFile", logFile.toFile());
      setField(stateMachine, "simpleLogWriter", writer);
      Mockito.doThrow(new IOException("injected force failure")).when(writer).force();
      Mockito.doThrow(new IOException("injected close failure")).when(writer).close();

      TSStatus status = stateMachine.write(versionPlan(1));

      Assert.assertEquals(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(), status.getCode());
      Assert.assertTrue(
          status
              .getMessage()
              .contains(
                  ConfigNodeMessages.FAILED_TO_ROLLBACK_PERSISTED_CONFIGNODE_SIMPLECONSENSUS_LOG));
      long failedLogSize = Files.size(logFile);
      Assert.assertTrue(failedLogSize > 0);
      Assert.assertEquals(
          TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(),
          stateMachine.write(versionPlan(2)).getCode());
      Assert.assertEquals(failedLogSize, Files.size(logFile));
      Mockito.verify(writer, Mockito.times(1)).write(Mockito.any(ByteBuffer.class));
      Mockito.verify(executor, Mockito.never())
          .executeNonQueryPlan(Mockito.any(ConfigPhysicalPlan.class));
    } finally {
      Mockito.doCallRealMethod().when(writer).close();
      closeSimpleLogWriter(stateMachine);
      Files.deleteIfExists(logFile);
      conf.setConfigNodeConsensusProtocolClass(originalConsensusProtocol);
    }
  }

  @Test
  public void testRotationRenameFailureReopensOriginalLog() throws Exception {
    ConfigNodeConfig conf = ConfigNodeDescriptor.getInstance().getConf();
    String originalConsensusProtocol = conf.getConfigNodeConsensusProtocolClass();
    Path directory = Files.createTempDirectory("confignode-rotation-failure");
    Path logFile = directory.resolve("log_inprogress_1");
    Path conflictingTarget = Files.createDirectory(directory.resolve("log_1_1"));
    Path blocker = Files.createFile(conflictingTarget.resolve("blocker"));
    ConfigPlanExecutor executor = Mockito.mock(ConfigPlanExecutor.class);
    Mockito.when(executor.executeNonQueryPlan(Mockito.any(ConfigPhysicalPlan.class)))
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    ConfigRegionStateMachine stateMachine = new ConfigRegionStateMachine(null, executor);
    ConfigPhysicalPlan firstPlan = versionPlan(1);
    ConfigPhysicalPlan retryPlan = versionPlan(2);
    try {
      conf.setConfigNodeConsensusProtocolClass(ConsensusFactory.SIMPLE_CONSENSUS);
      setField(stateMachine, "simpleLogFile", logFile.toFile());
      setField(stateMachine, "simpleLogWriter", new LogWriter(logFile.toFile(), false));
      setField(stateMachine, "startIndex", 1);
      stateMachine.write(firstPlan);
      Method rotate =
          ConfigRegionStateMachine.class.getDeclaredMethod("rollSimpleConsensusLogFile");
      rotate.setAccessible(true);
      try {
        rotate.invoke(stateMachine);
        Assert.fail("Expected the conflicting target to prevent renaming");
      } catch (InvocationTargetException e) {
        Assert.assertTrue(e.getCause() instanceof IOException);
      }

      Assert.assertNotNull(getField(stateMachine, "simpleLogWriter"));
      Assert.assertEquals(1, getField(stateMachine, "startIndex"));
      Assert.assertEquals(1, getField(stateMachine, "endIndex"));
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), stateMachine.write(retryPlan).getCode());
      closeSimpleLogWriter(stateMachine);
      assertReplayablePlans(logFile, firstPlan, retryPlan);
    } finally {
      closeSimpleLogWriter(stateMachine);
      Files.deleteIfExists(logFile);
      Files.deleteIfExists(blocker);
      Files.deleteIfExists(conflictingTarget);
      Files.deleteIfExists(directory);
      conf.setConfigNodeConsensusProtocolClass(originalConsensusProtocol);
    }
  }

  private static ConfigPhysicalPlan versionPlan(int nodeId) {
    return new UpdateVersionInfoPlan(new TNodeVersionInfo("version", "build"), nodeId);
  }

  private static void assertReplayablePlans(Path logFile, ConfigPhysicalPlan... plans)
      throws Exception {
    SingleFileLogReader reader = new SingleFileLogReader(logFile.toFile());
    try {
      for (ConfigPhysicalPlan plan : plans) {
        Assert.assertTrue(reader.hasNext());
        Assert.assertEquals(plan, reader.next());
      }
      Assert.assertFalse(reader.hasNext());
    } finally {
      reader.close();
    }
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
