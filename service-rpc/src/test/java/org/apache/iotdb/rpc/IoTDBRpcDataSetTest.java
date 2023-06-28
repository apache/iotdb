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

package org.apache.iotdb.rpc;

import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TsBlockSerde;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class IoTDBRpcDataSetTest {
  private final TsBlockSerde serde = new TsBlockSerde();

  @Test
  public void nullTsBlockTest()
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    ByteBuffer data = serde.serialize(new TsBlock(0, new TimeColumn(0, new long[0])));
    ByteBuffer data2 = serde.serialize(new TsBlock(1, new TimeColumn(1, new long[1])));
    ArrayList<ByteBuffer> bufferArrayList = new ArrayList<>();
    bufferArrayList.add(data);
    bufferArrayList.add(data2);
    IoTDBRpcDataSet ioTDBRpcDataSet =
        new IoTDBRpcDataSet(
            "",
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyMap(),
            false,
            false,
            0,
            0,
            null,
            0,
            bufferArrayList,
            0,
            0);

    int count = 0;
    while (ioTDBRpcDataSet.next()) {
      ioTDBRpcDataSet.getLong(1);
      count++;
    }
    assertEquals(1, count);
  }

  @Test
  public void nullTsBlockTest2()
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    ByteBuffer data = serde.serialize(new TsBlock(0, new TimeColumn(0, new long[0])));
    ArrayList<ByteBuffer> bufferArrayList = new ArrayList<>();
    bufferArrayList.add(data);
    IoTDBRpcDataSet ioTDBRpcDataSet =
        new IoTDBRpcDataSet(
            "",
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyMap(),
            false,
            false,
            0,
            0,
            null,
            0,
            bufferArrayList,
            0,
            0);
    int count = 0;
    while (ioTDBRpcDataSet.next()) {
      ioTDBRpcDataSet.getLong(1);
      count++;
    }
    assertEquals(0, count);
  }
}
