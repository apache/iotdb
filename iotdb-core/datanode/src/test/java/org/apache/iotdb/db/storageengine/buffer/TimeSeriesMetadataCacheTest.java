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

package org.apache.iotdb.db.storageengine.buffer;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.storageengine.buffer.TimeSeriesMetadataCache.TimeSeriesMetadataCacheKey;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TimeSeriesMetadataCacheTest {

  private void testCachePlaceHolderInternal() throws IOException, WriteProcessException {
    TimeSeriesMetadataCache.getInstance().clear();
    File file = new File("target/test.tsfile");
    TsFileID tsFileID = new TsFileID();

    int deviceCnt = 100;
    int seriesPerDevice = 100;
    List<IDeviceID> deviceIDList = new ArrayList<>();
    for (int i = 0; i < deviceCnt; i++) {
      deviceIDList.add(Factory.DEFAULT_FACTORY.create("root.d" + i));
    }

    try (TsFileWriter tsFileWriter = new TsFileWriter(file)) {
      // 100*100 series in the file

      for (int i = 0; i < deviceCnt; i++) {
        for (int j = 0; j < seriesPerDevice; j++) {
          tsFileWriter.registerTimeseries(
              deviceIDList.get(i), new MeasurementSchema("s" + j, TSDataType.INT32));
        }
      }
      for (int i = 0; i < deviceCnt; i++) {
        TSRecord rec = new TSRecord(deviceIDList.get(i), 0);
        for (int j = 0; j < seriesPerDevice; j++) {
          rec.addPoint("s" + j, 0);
        }
        tsFileWriter.writeRecord(rec);
      }
      tsFileWriter.close();

      // read 100*200 series each 10 times in the file
      TimeSeriesMetadataCache.getInstance().clear();
      long start = System.currentTimeMillis();
      QueryContext queryContext = new QueryContext();
      // put k in outer loop
      for (int k = 0; k < 10; k++) {
        for (int i = 0; i < deviceCnt; i++) {
          for (int j = 0; j < seriesPerDevice; j++) {
            TimeSeriesMetadataCacheKey key =
                new TimeSeriesMetadataCacheKey(tsFileID, deviceIDList.get(i), "s" + j);
            TimeSeriesMetadataCache.getInstance()
                .get(file.getPath(), key, Collections.EMPTY_SET, true, false, queryContext);
          }
        }
      }
      System.out.println("time cost with outer k: " + (System.currentTimeMillis() - start));

      TimeSeriesMetadataCache.getInstance().clear();
      start = System.currentTimeMillis();
      queryContext = new QueryContext();
      // put k in inner loop
      for (int i = 0; i < deviceCnt; i++) {
        for (int j = 0; j < seriesPerDevice; j++) {
          TimeSeriesMetadataCacheKey key =
              new TimeSeriesMetadataCacheKey(tsFileID, deviceIDList.get(i), "s" + j);
          for (int k = 0; k < 10; k++) {
            TimeSeriesMetadataCache.getInstance()
                .get(file.getPath(), key, Collections.EMPTY_SET, true, false, queryContext);
          }
        }
      }
      System.out.println("time cost with inner k: " + (System.currentTimeMillis() - start));
    } finally {
      file.delete();
    }
  }

  @Ignore("Performance")
  @Test
  public void testCachePlaceHolder() throws IOException, WriteProcessException {
    boolean mayCacheNonExistSeries =
        IoTDBDescriptor.getInstance().getMemoryConfig().isMayCacheNonExistSeries();
    try {
      System.out.println("warming up");
      System.out.println("Do not cache non-exist series");
      IoTDBDescriptor.getInstance().getMemoryConfig().setMayCacheNonExistSeries(false);
      testCachePlaceHolderInternal();
      System.out.println("Cache non-exist series");
      IoTDBDescriptor.getInstance().getMemoryConfig().setMayCacheNonExistSeries(true);
      testCachePlaceHolderInternal();

      System.out.println("actual test");
      System.out.println("Do not cache non-exist series");
      IoTDBDescriptor.getInstance().getMemoryConfig().setMayCacheNonExistSeries(false);
      testCachePlaceHolderInternal();
      System.out.println("Cache non-exist series");
      IoTDBDescriptor.getInstance().getMemoryConfig().setMayCacheNonExistSeries(true);
      testCachePlaceHolderInternal();
    } finally {
      IoTDBDescriptor.getInstance()
          .getMemoryConfig()
          .setMayCacheNonExistSeries(mayCacheNonExistSeries);
    }
  }
}
