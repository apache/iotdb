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
package org.apache.iotdb.hadoop.tsfile;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.iotdb.hadoop.tsfile.TSFInputSplit;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

/**
 * Test the {@link TSFInputSplit}
 * Assert the readFields function and write function is right
 */
public class TSFInputSplitTest {

  private TSFInputSplit wInputSplit;
  private TSFInputSplit rInputSplit;
  private DataInputBuffer dataInputBuffer = new DataInputBuffer();
  private DataOutputBuffer dataOutputBuffer = new DataOutputBuffer();

  @Before
  public void setUp() throws Exception {
    // For the test data
    Path path = new Path("input");
    int numOfRowGroupMetaDate = 1;
    List<TSFInputSplit.ChunkGroupInfo> chunkGroupInfoList = new ArrayList<>();
    chunkGroupInfoList
            .add(new TSFInputSplit.ChunkGroupInfo("d1", new String[] {"sensor_0", "sensor_1"},1, 10));
    chunkGroupInfoList
            .add(new TSFInputSplit.ChunkGroupInfo("d1", new String[] {"sensor_0", "sensor_1"},11, 20));
    chunkGroupInfoList
            .add(new TSFInputSplit.ChunkGroupInfo("d2", new String[] {"sensor_0", "sensor_1"},21, 30));
    long length = 100;
    String[] hosts = {"192.168.1.1", "192.168.1.0", "localhost"};

    wInputSplit = new TSFInputSplit(path, hosts, 30, chunkGroupInfoList);
    rInputSplit = new TSFInputSplit();
  }

  @Test
  public void testInputSplitWriteAndRead() {
    try {
      // call the write method to serialize the object
      wInputSplit.write(dataOutputBuffer);
      dataOutputBuffer.flush();
      dataInputBuffer.reset(dataOutputBuffer.getData(), dataOutputBuffer.getLength());
      rInputSplit.readFields(dataInputBuffer);
      dataInputBuffer.close();
      dataOutputBuffer.close();
      // assert
      assertEquals(wInputSplit.getPath(), rInputSplit.getPath());
      assertThat(wInputSplit.getChunkGroupInfoList(), is(rInputSplit.getChunkGroupInfoList()));

      assertEquals(wInputSplit.getLength(), rInputSplit.getLength());
      assertArrayEquals(wInputSplit.getLocations(), rInputSplit.getLocations());
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

}
