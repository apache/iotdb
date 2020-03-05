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
package org.apache.iotdb.tsfile.file.metadata.utils;

import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.header.PageHeaderTest;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public class TestHelper {

  public static TsFileMetaData createSimpleFileMetaData() {
    TsFileMetaData metaData = new TsFileMetaData();
    metaData.setDeviceMetaDataMap(generateDeviceMetaDataMap());
    return metaData;
  }

  private static Map<String, Pair<Long, Integer>> generateDeviceMetaDataMap() {
    Map<String, Pair<Long, Integer>> deviceMetaDataMap = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      deviceMetaDataMap.put("d" + i, new Pair<Long, Integer>((long) i * 5, 5));
    }
    return deviceMetaDataMap;
  }

  public static MeasurementSchema createSimpleMeasurementSchema(String measurementuid) {
    return new MeasurementSchema(measurementuid, TSDataType.INT64, TSEncoding.RLE);
  }
  
  public static TimeseriesMetaData createSimpleTimseriesMetaData(String measurementuid) {
    Statistics<?> statistics = Statistics.getStatsByType(PageHeaderTest.DATA_TYPE);
    statistics.setEmpty(false);
    TimeseriesMetaData timeseriesMetaData = new TimeseriesMetaData();
    timeseriesMetaData.setMeasurementId(measurementuid);
    timeseriesMetaData.setTSDataType(PageHeaderTest.DATA_TYPE);
    timeseriesMetaData.setOffsetOfChunkMetaDataList(1000L);
    timeseriesMetaData.setDataSizeOfChunkMetaDataList(200);
    timeseriesMetaData.setStatistics(statistics);
    return timeseriesMetaData;
  }

  public static PageHeader createTestPageHeader() {
    Statistics<?> statistics = Statistics.getStatsByType(PageHeaderTest.DATA_TYPE);
    statistics.setEmpty(false);
    return new PageHeader(PageHeaderTest.UNCOMPRESSED_SIZE, PageHeaderTest.COMPRESSED_SIZE,
        statistics);
  }
}
