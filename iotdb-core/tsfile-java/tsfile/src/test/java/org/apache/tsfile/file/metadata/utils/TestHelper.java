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
package org.apache.tsfile.file.metadata.utils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.header.PageHeaderTest;
import org.apache.tsfile.file.metadata.DeviceMetadataIndexEntry;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.TsFileMetadata;
import org.apache.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;

public class TestHelper {

  public static final String TEST_TABLE_NAME = "test_table";

  public static TsFileMetadata createSimpleFileMetaData() {
    TsFileMetadata metaData = new TsFileMetadata();
    metaData.setTableMetadataIndexNodeMap(
        Collections.singletonMap(TEST_TABLE_NAME, generateMetaDataIndex()));
    return metaData;
  }

  private static MetadataIndexNode generateMetaDataIndex() {
    MetadataIndexNode metaDataIndex = new MetadataIndexNode(MetadataIndexNodeType.LEAF_DEVICE);
    for (int i = 0; i < 5; i++) {
      metaDataIndex.addEntry(
          new DeviceMetadataIndexEntry(Factory.DEFAULT_FACTORY.create("d" + i), (long) i * 5));
    }
    return metaDataIndex;
  }

  public static MeasurementSchema createSimpleMeasurementSchema(String measurementuid) {
    return new MeasurementSchema(measurementuid, TSDataType.INT64, TSEncoding.RLE);
  }

  public static TimeseriesMetadata createSimpleTimeseriesMetaData(String measurementuid) {
    Statistics<? extends Serializable> statistics =
        Statistics.getStatsByType(PageHeaderTest.DATA_TYPE);
    statistics.setEmpty(false);
    TimeseriesMetadata timeseriesMetaData = new TimeseriesMetadata();
    timeseriesMetaData.setMeasurementId(measurementuid);
    timeseriesMetaData.setTsDataType(PageHeaderTest.DATA_TYPE);
    timeseriesMetaData.setDataSizeOfChunkMetaDataList(0);
    timeseriesMetaData.setChunkMetadataListBuffer(new PublicBAOS());
    timeseriesMetaData.setStatistics(statistics);
    timeseriesMetaData.setChunkMetadataList(new ArrayList<>());
    return timeseriesMetaData;
  }

  public static PageHeader createTestPageHeader() {
    Statistics<? extends Serializable> statistics =
        Statistics.getStatsByType(PageHeaderTest.DATA_TYPE);
    statistics.setEmpty(false);
    return new PageHeader(
        PageHeaderTest.UNCOMPRESSED_SIZE, PageHeaderTest.COMPRESSED_SIZE, statistics);
  }
}
