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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Map;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsDigest;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;

public class Utils {

  private static final double maxError = 0.0001d;


  public static void isListEqual(List<?> listA, List<?> listB, String name) {
    if ((listA == null) ^ (listB == null)) {
      System.out.println("error");
      fail(String.format("one of %s is null", name));
    }
    if ((listA != null) && (listB != null)) {
      if (listA.size() != listB.size()) {
        fail(String.format("%s size is different", name));
      }
      for (int i = 0; i < listA.size(); i++) {
        assertTrue(listA.get(i).equals(listB.get(i)));
      }
    }
  }

  public static void isMapStringEqual(Map<String, String> mapA, Map<String, String> mapB,
      String name) {
    if ((mapA == null) ^ (mapB == null)) {
      System.out.println("error");
      fail(String.format("one of %s is null", name));
    }
    if ((mapA != null) && (mapB != null)) {
      if (mapA.size() != mapB.size()) {
        fail(String.format("%s size is different", name));
      }
      for (String key : mapA.keySet()) {
        assertTrue(mapA.get(key).equals(mapB.get(key)));
      }
    }
  }

  public static void isTwoTsDigestEqual(TsDigest digestA, TsDigest digestB, String name) {
    if ((digestA == null) ^ (digestB == null)) {
      System.out.println("error");
      fail(String.format("one of %s is null", name));
    }
    if (digestA != null) {
      Assert.assertEquals(digestA, digestB);
    }
  }

  /**
   * when one of A and B is Null, A != B, so test case fails.
   *
   * @return false - A and B both are NULL, so we do not need to check whether their members are
   * equal true - A and B both are not NULL, so we need to check their members
   */
  public static boolean isTwoObjectsNotNULL(Object objectA, Object objectB, String name) {
    if ((objectA == null) && (objectB == null)) {
      return false;
    }
    if ((objectA == null) ^ (objectB == null)) {
      fail(String.format("one of %s is null", name));
    }
    return true;
  }

  public static void isStringSame(Object str1, Object str2, String name) {
    if ((str1 == null) && (str2 == null)) {
      return;
    }
    if ((str1 == null) ^ (str2 == null)) {
      fail(String.format("one of %s string is null", name));
    }
    assertTrue(str1.toString().equals(str2.toString()));
  }

  public static void isTimeSeriesChunkMetadataEqual(ChunkMetaData metadata1,
      ChunkMetaData metadata2) {
    if (Utils.isTwoObjectsNotNULL(metadata1, metadata2, "ChunkMetaData")) {
      if (Utils.isTwoObjectsNotNULL(metadata1.getMeasurementUid(), metadata2.getMeasurementUid(),
          "sensorUID")) {
        assertTrue(metadata1.getMeasurementUid().equals(metadata2.getMeasurementUid()));
      }
      assertTrue(metadata1.getOffsetOfChunkHeader() == metadata2.getOffsetOfChunkHeader());
      assertTrue(metadata1.getNumOfPoints() == metadata2.getNumOfPoints());
      assertTrue(metadata1.getStartTime() == metadata2.getStartTime());
      assertTrue(metadata1.getEndTime() == metadata2.getEndTime());
      assertNotNull(metadata1.getDigest());
      assertNotNull(metadata2.getDigest());
      Utils.isTwoTsDigestEqual(metadata1.getDigest(), metadata2.getDigest(), "TsDigest");
    }
  }

  public static void isTsDeviceMetadataEqual(TsDeviceMetadata metadata1,
      TsDeviceMetadata metadata2) {
    if (Utils.isTwoObjectsNotNULL(metadata1, metadata2, "DeviceMetaData")) {
      assertEquals(metadata1.getStartTime(), metadata2.getStartTime());
      assertEquals(metadata1.getEndTime(), metadata2.getEndTime());

      if (Utils.isTwoObjectsNotNULL(metadata1.getChunkGroupMetaDataList(),
          metadata2.getChunkGroupMetaDataList(),
          "Rowgroup metadata list")) {
        assertEquals(metadata1.getChunkGroupMetaDataList().size(),
            metadata2.getChunkGroupMetaDataList().size());
        for (int i = 0; i < metadata1.getChunkGroupMetaDataList().size(); i++) {
          Utils.isChunkGroupMetaDataEqual(metadata1.getChunkGroupMetaDataList().get(i),
              metadata1.getChunkGroupMetaDataList().get(i));
        }
      }
    }
  }

  public static void isChunkGroupMetaDataEqual(ChunkGroupMetaData metadata1,
      ChunkGroupMetaData metadata2) {
    if (Utils.isTwoObjectsNotNULL(metadata1, metadata2, "ChunkGroupMetaData")) {
      assertTrue(metadata1.getDeviceID().equals(metadata2.getDeviceID()));

      if (Utils
          .isTwoObjectsNotNULL(metadata1.getChunkMetaDataList(), metadata2.getChunkMetaDataList(),
              "Timeseries chunk metadata list")) {
        assertEquals(metadata1.getChunkMetaDataList().size(),
            metadata2.getChunkMetaDataList().size());
        for (int i = 0; i < metadata1.getChunkMetaDataList().size(); i++) {
          Utils.isTimeSeriesChunkMetadataEqual(metadata1.getChunkMetaDataList().get(i),
              metadata1.getChunkMetaDataList().get(i));
        }
      }
    }
  }

  public static void isTsDeviceMetadataIndexEqual(TsDeviceMetadataIndex index1,
      TsDeviceMetadataIndex index2) {
    if (Utils.isTwoObjectsNotNULL(index1, index2, "TsDeviceMetadataIndex")) {
      assertEquals(index1.getOffset(), index2.getOffset());
      assertEquals(index1.getLen(), index2.getLen());
      assertEquals(index1.getStartTime(), index2.getStartTime());
      assertEquals(index1.getEndTime(), index2.getEndTime());
    }
  }

  public static void isFileMetaDataEqual(TsFileMetaData metadata1, TsFileMetaData metadata2) {
    if (Utils.isTwoObjectsNotNULL(metadata1, metadata2, "File MetaData")) {
      if (Utils.isTwoObjectsNotNULL(metadata1.getDeviceMap(), metadata2.getDeviceMap(),
          "Delta object metadata list")) {

        Map<String, TsDeviceMetadataIndex> deviceMetadataMap1 = metadata1.getDeviceMap();
        Map<String, TsDeviceMetadataIndex> deviceMetadataMap2 = metadata2.getDeviceMap();
        assertEquals(deviceMetadataMap1.size(), deviceMetadataMap2.size());

        for (String key : deviceMetadataMap1.keySet()) {
          Utils.isTsDeviceMetadataIndexEqual(deviceMetadataMap1.get(key),
              deviceMetadataMap2.get(key));
        }
      }

      if (Utils
          .isTwoObjectsNotNULL(metadata1.getMeasurementSchema(), metadata2.getMeasurementSchema(),
              "Timeseries metadata list")) {
        assertEquals(metadata1.getMeasurementSchema().size(),
            metadata2.getMeasurementSchema().size());
        for (Map.Entry<String, MeasurementSchema> entry : metadata1.getMeasurementSchema()
            .entrySet()) {
          entry.getValue().equals(metadata2.getMeasurementSchema().get(entry.getKey()));
        }
      }

      assertEquals(metadata1.getCurrentVersion(), metadata2.getCurrentVersion());
      assertEquals(metadata1.getCreatedBy(), metadata2.getCreatedBy());
    }
  }

  public static void isPageHeaderEqual(PageHeader header1, PageHeader header2) {
    if (Utils.isTwoObjectsNotNULL(header1, header2, "PageHeader")) {
      assertTrue(header1.getUncompressedSize() == header2.getUncompressedSize());
      assertTrue(header1.getCompressedSize() == header2.getCompressedSize());
      assertTrue(header1.getNumOfValues() == header2.getNumOfValues());
      assertTrue(header1.getMaxTimestamp() == header2.getMaxTimestamp());
      assertTrue(header1.getMinTimestamp() == header2.getMinTimestamp());
      if (Utils
          .isTwoObjectsNotNULL(header1.getStatistics(), header2.getStatistics(), "statistics")) {
        Utils.isStatisticsEqual(header1.getStatistics(), header2.getStatistics());
      }
    }
  }

  public static void isStatisticsEqual(Statistics statistics1, Statistics statistics2) {
    if ((statistics1 == null) ^ (statistics2 == null)) {
      System.out.println("error");
      fail("one of statistics is null");
    }
    if ((statistics1 != null) && (statistics2 != null)) {
      if (statistics1.isEmpty() ^ statistics2.isEmpty()) {
        fail("one of statistics is empty while the other one is not");
      }
      if (!statistics1.isEmpty() && !statistics2.isEmpty()) {
        assertEquals(statistics1.getMin(), statistics2.getMin());
        assertEquals(statistics1.getMax(), statistics2.getMax());
        assertEquals(statistics1.getFirst(), statistics2.getFirst());
        assertEquals(statistics1.getSum(), statistics2.getSum(), maxError);
        assertEquals(statistics1.getLast(), statistics2.getLast());
      }
    }
  }
}