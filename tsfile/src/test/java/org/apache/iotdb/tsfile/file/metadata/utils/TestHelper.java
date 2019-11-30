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
import org.apache.iotdb.tsfile.file.metadata.TimeSeriesMetadataTest;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaDataTest;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public class TestHelper {

  public static TsFileMetaData createSimpleFileMetaData() {
    TsFileMetaData metaData = new TsFileMetaData(generateDeviceIndexMetadataMap(), new HashMap<>());
    metaData.addMeasurementSchema(TestHelper.createSimpleMeasurementSchema());
    metaData.addMeasurementSchema(TestHelper.createSimpleMeasurementSchema());
    metaData.setCreatedBy(TsFileMetaDataTest.CREATED_BY);
    return metaData;
  }

  private static Map<String, TsDeviceMetadataIndex> generateDeviceIndexMetadataMap() {
    Map<String, TsDeviceMetadataIndex> indexMap = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      indexMap.put("device_" + i, createSimpleDeviceIndexMetadata());
    }
    return indexMap;
  }

  private static TsDeviceMetadataIndex createSimpleDeviceIndexMetadata() {
    TsDeviceMetadataIndex index = new TsDeviceMetadataIndex();
    index.setOffset(0);
    index.setLen(10);
    index.setStartTime(100);
    index.setEndTime(200);
    return index;
  }

  public static MeasurementSchema createSimpleMeasurementSchema() {
    return new MeasurementSchema(TimeSeriesMetadataTest.measurementUID,
        TSDataType.INT64,
        TSEncoding.RLE);
  }


  public static PageHeader createTestPageHeader() {
    Statistics<?> statistics = Statistics.getStatsByType(PageHeaderTest.DATA_TYPE);
    statistics.setEmpty(false);
    return new PageHeader(PageHeaderTest.UNCOMPRESSED_SIZE,
        PageHeaderTest.COMPRESSED_SIZE, PageHeaderTest.NUM_OF_VALUES,
        statistics, PageHeaderTest.MAX_TIMESTAMO, PageHeaderTest.MIN_TIMESTAMO);
  }
}
