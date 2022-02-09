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

import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.header.PageHeaderTest;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexEntry;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexNode;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import java.io.Serializable;

public class TestHelper {

  public static TsFileMetadata createSimpleFileMetaData() {
    TsFileMetadata metaData = new TsFileMetadata();
    metaData.setMetadataIndex(generateMetaDataIndex());
    return metaData;
  }

  private static MetadataIndexNode generateMetaDataIndex() {
    MetadataIndexNode metaDataIndex = new MetadataIndexNode(MetadataIndexNodeType.LEAF_MEASUREMENT);
    for (int i = 0; i < 5; i++) {
      metaDataIndex.addEntry(new MetadataIndexEntry("d" + i, (long) i * 5));
    }
    return metaDataIndex;
  }

  public static UnaryMeasurementSchema createSimpleMeasurementSchema(String measurementuid) {
    return new UnaryMeasurementSchema(measurementuid, TSDataType.INT64, TSEncoding.RLE);
  }

  public static TimeseriesMetadata createSimpleTimseriesMetaData(String measurementuid) {
    Statistics<? extends Serializable> statistics =
        Statistics.getStatsByType(PageHeaderTest.DATA_TYPE);
    statistics.setEmpty(false);
    TimeseriesMetadata timeseriesMetaData = new TimeseriesMetadata();
    timeseriesMetaData.setMeasurementId(measurementuid);
    timeseriesMetaData.setTSDataType(PageHeaderTest.DATA_TYPE);
    timeseriesMetaData.setOffsetOfChunkMetaDataList(1000L);
    timeseriesMetaData.setDataSizeOfChunkMetaDataList(0);
    timeseriesMetaData.setChunkMetadataListBuffer(new PublicBAOS());
    timeseriesMetaData.setStatistics(statistics);
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
