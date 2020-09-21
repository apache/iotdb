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

package org.apache.iotdb.cluster.utils;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.exception.UnknownLogTypeException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogParser;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.junit.Test;

public class SerializeUtilTest {

  @Test
  public void testStrToNode() {
    for (int i = 0; i < 10; i++) {
      Node node = TestUtils.getNode(i);
      String nodeStr = node.toString();
      Node fromStr = ClusterUtils.stringToNode(nodeStr);
      assertEquals(node, fromStr);
    }
  }

  @Test
  public void testInsertTabletPlanLog()
      throws UnknownLogTypeException, IOException, IllegalPathException {
    long[] times = new long[]{110L, 111L, 112L, 113L};
    List<Integer> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.DOUBLE.ordinal());
    dataTypes.add(TSDataType.INT64.ordinal());
    dataTypes.add(TSDataType.TEXT.ordinal());
    dataTypes.add(TSDataType.BOOLEAN.ordinal());
    Object[] columns = new Object[4];
    columns[0] = new double[4];
    columns[1] = new long[4];
    columns[2] = new Binary[4];
    columns[3] = new boolean[4];

    for (int r = 0; r < 4; r++) {
      ((double[]) columns[0])[r] = 1.0;
      ((long[]) columns[1])[r] = 1;
      ((Binary[]) columns[2])[r] = new Binary("hh" + r);
      ((boolean[]) columns[3])[r] = false;
    }

    InsertTabletPlan tabletPlan = new InsertTabletPlan(new PartialPath("root.test"),
        new String[]{"s1", "s2", "s3", "s4"}, dataTypes);
    tabletPlan.setTimes(times);
    tabletPlan.setColumns(columns);
    tabletPlan.setRowCount(times.length);
    tabletPlan.setStart(0);
    tabletPlan.setEnd(4);

    Log log = new PhysicalPlanLog(tabletPlan);
    log.setCurrLogTerm(1);
    log.setCurrLogIndex(2);

    ByteBuffer serialized = log.serialize();

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    tabletPlan.serialize(dataOutputStream);
    ByteBuffer bufferA = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
    ByteBuffer bufferB = ByteBuffer.allocate(bufferA.limit());
    tabletPlan.serialize(bufferB);
    bufferB.flip();
    assertEquals(bufferA, bufferB);

    Log parsed = LogParser.getINSTANCE().parse(serialized);
    assertEquals(log, parsed);
  }
}
