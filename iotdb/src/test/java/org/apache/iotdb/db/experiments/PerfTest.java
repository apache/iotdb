/**
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

package org.apache.iotdb.db.experiments;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.IWritableMemChunk;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.chunk.ChunkBuffer;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.Test;

public class PerfTest {
  int fullSize = 128*1024*1024/16;
  int deviceSize = 1000;
  int sensorSize = 1000;
  long time = 100L;
  Random random = new Random(System.currentTimeMillis());
  Map<String, Long> maxTimeStamp = new HashMap<>();
  IMemTable table = new PrimitiveMemTable();
  @Test
  public void test2(){

    long start = System.currentTimeMillis();
    for ( int i =0 ; i< fullSize; i++ ) {
      time = random.nextInt(10) - 5 <0 ? time + random.nextInt(10) : time - random.nextInt(10);
      String device = "d" + random.nextInt(deviceSize);
      String sensor = "s" + random.nextInt(sensorSize);
      table.write(device, sensor, TSDataType.FLOAT, time, 5.0f);
      if (!maxTimeStamp.containsKey(device) || maxTimeStamp.get(device) < time) {
        maxTimeStamp.put(device, time);
      }
    }
    for (String deviceId : table.getMemTableMap().keySet()) {
      for (String measurementId : table.getMemTableMap().get(deviceId).keySet()) {
        IWritableMemChunk series = table.getMemTableMap().get(deviceId).get(measurementId);
        series.getSortedTimeValuePairList();
      }
    }
    System.out.println("origin total time cost: " + (System.currentTimeMillis() - start));
  }

  @Test
  public void test(){
    long start = System.currentTimeMillis();

    for ( int i =0 ; i< fullSize; i++ ) {
      time = random.nextInt(10) - 5 <0 ? time + random.nextInt(10) : time - random.nextInt(10);
      String device = "d" + random.nextInt(deviceSize);
      String sensor = "s" + random.nextInt(sensorSize);
      table.write(device, sensor, TSDataType.FLOAT, time, 5.0f);

    }
    for (String deviceId : table.getMemTableMap().keySet()) {
      long minTime = Long.MAX_VALUE;
      long maxTime = Long.MIN_VALUE;
      for (String measurementId : table.getMemTableMap().get(deviceId).keySet()) {
        IWritableMemChunk series = table.getMemTableMap().get(deviceId).get(measurementId);

          List<TimeValuePair> sortedTimeValuePairs = series.getSortedTimeValuePairList();
          if (sortedTimeValuePairs.get(sortedTimeValuePairs.size() - 1).getTimestamp() > maxTime) {
            maxTime = sortedTimeValuePairs.get(sortedTimeValuePairs.size() - 1).getTimestamp();
          }

      }
      maxTimeStamp.put(deviceId, maxTime);
    }
    System.out.println("modified total time cost: " + (System.currentTimeMillis() - start));
  }

//  @Test
//  public void test3() {
//    IMemTable table = new PrimitiveMemTable();
//    table.write("d1", "s1", TSDataType.FLOAT, 10, 5.0f);
//    table.write("d1", "s1", TSDataType.FLOAT, 1, 5.0f);
//    ReadOnlyMemChunk chunk = table.query("d1", "s1", TSDataType.FLOAT, Collections.emptyMap());
//    chunk.getSortedTimeValuePairList().forEach( x -> System.out.println(x.getTimestamp()));
//  }
//
//
//  private void scan(IMemTable imemTable) {
//    Map<String, Pair<Long, Long>> result = new HashMap<>();
//    for (String deviceId : imemTable.getMemTableMap().keySet()) {
//      int seriesNumber = imemTable.getMemTableMap().get(deviceId).size();
//      long minTime = Long.MAX_VALUE;
//      long maxTime = Long.MIN_VALUE;
//      for (String measurementId : imemTable.getMemTableMap().get(deviceId).keySet()) {
//        // TODO if we can not use TSFileIO writer, then we have to redesign the class of TSFileIO.
//        IWritableMemChunk series = imemTable.getMemTableMap().get(deviceId).get(measurementId);
//        List<TimeValuePair> sortedTimeValuePairs = series.getSortedTimeValuePairList();
//        if (sortedTimeValuePairs.get(0).getTimestamp() < minTime) {
//          minTime = sortedTimeValuePairs.get(0).getTimestamp();
//        }
//        if (sortedTimeValuePairs.get(sortedTimeValuePairs.size() - 1).getTimestamp() > maxTime) {
//          maxTime = sortedTimeValuePairs.get(sortedTimeValuePairs.size() - 1).getTimestamp();
//        }
//      }
//      long memSize = tsFileIoWriter.getPos() - startPos;
//      ChunkGroupFooter footer = new ChunkGroupFooter(deviceId, memSize, seriesNumber);
//      tsFileIoWriter.endChunkGroup(footer, version);
//      result.put(deviceId, new Pair<>(minTime, maxTime));
//    }
//  }
}
