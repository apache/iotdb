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

package org.apache.iotdb.cluster.log;

import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.exception.UnknownLogTypeException;
import org.apache.iotdb.cluster.log.logtypes.AddNodeLog;
import org.apache.iotdb.cluster.log.logtypes.CloseFileLog;
import org.apache.iotdb.cluster.log.logtypes.EmptyContentLog;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.apache.iotdb.cluster.log.logtypes.RemoveNodeLog;
import org.apache.iotdb.cluster.utils.PlanSerializer;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.LogPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class LogParserTest {

  private LogParser logParser = LogParser.getINSTANCE();

  @Test
  public void testAddNodeLog() throws UnknownLogTypeException {
    AddNodeLog log = new AddNodeLog();
    log.setNewNode(TestUtils.getNode(5));
    log.setPartitionTable(TestUtils.seralizePartitionTable);
    log.setCurrLogIndex(8);
    log.setCurrLogTerm(8);

    ByteBuffer buffer = log.serialize();
    Log serialized = logParser.parse(buffer);
    assertEquals(log, serialized);
  }

  @Test
  public void testPhysicalPlanLog() throws UnknownLogTypeException, IllegalPathException {
    PhysicalPlanLog log = new PhysicalPlanLog();
    SetStorageGroupPlan setStorageGroupPlan =
        new SetStorageGroupPlan(new PartialPath(TestUtils.getTestSg(5)));
    log.setPlan(setStorageGroupPlan);
    log.setCurrLogIndex(8);
    log.setCurrLogTerm(8);

    ByteBuffer buffer = log.serialize();
    Log serialized = logParser.parse(buffer);
    assertEquals(log, serialized);
  }

  @Test
  public void testCloseFileLog() throws UnknownLogTypeException {
    CloseFileLog log = new CloseFileLog(TestUtils.getTestSg(5), 0, false);
    log.setCurrLogIndex(8);
    log.setCurrLogTerm(8);

    ByteBuffer buffer = log.serialize();
    Log serialized = logParser.parse(buffer);
    assertEquals(log, serialized);
  }

  @Test
  public void testRemoveNodeLog() throws UnknownLogTypeException {
    RemoveNodeLog log = new RemoveNodeLog();
    log.setPartitionTable(TestUtils.seralizePartitionTable);
    log.setRemovedNode(TestUtils.getNode(0));
    log.setCurrLogIndex(8);
    log.setCurrLogTerm(8);

    ByteBuffer buffer = log.serialize();
    Log serialized = logParser.parse(buffer);
    assertEquals(log, serialized);
  }

  @Test
  public void testEmptyContentLog() throws UnknownLogTypeException {
    EmptyContentLog log = new EmptyContentLog();
    log.setCurrLogIndex(8);
    log.setCurrLogTerm(8);
    ByteBuffer byteBuffer = log.serialize();
    Log serialized = logParser.parse(byteBuffer);
    assertEquals(log, serialized);
  }

  @Test
  public void testLogPlan() {
    AddNodeLog log = new AddNodeLog(TestUtils.seralizePartitionTable, TestUtils.getNode(0));
    log.setMetaLogIndex(1);
    try {
      LogPlan logPlan = new LogPlan(log.serialize());
      ByteBuffer buffer = ByteBuffer.wrap(PlanSerializer.getInstance().serialize(logPlan));
      PhysicalPlan plan = PhysicalPlan.Factory.create(buffer);
      LogParser.getINSTANCE().parse(((LogPlan) plan).getLog());
    } catch (IllegalPathException | IOException | UnknownLogTypeException e) {
      fail();
    }
  }
}
