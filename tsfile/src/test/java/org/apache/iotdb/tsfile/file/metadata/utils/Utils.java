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
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexNode;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.apache.iotdb.tsfile.file.metadata.statistics.BooleanStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.IntegerStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
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

  public static void isTwoTsDigestEqual(Statistics statisticsA, Statistics statisticsB,
      String name) {
    if ((statisticsA == null) ^ (statisticsB == null)) {
      System.out.println("error");
      fail(String.format("one of %s is null", name));
    }
    if (statisticsA != null) {
      Assert.assertEquals(statisticsA, statisticsB);
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

  public static void isTimeSeriesChunkMetadataEqual(ChunkMetadata metadata1,
      ChunkMetadata metadata2) {
    if (Utils.isTwoObjectsNotNULL(metadata1, metadata2, "ChunkMetaData")) {
      assertTrue(metadata1.getOffsetOfChunkHeader() == metadata2.getOffsetOfChunkHeader());
      assertTrue(metadata1.getNumOfPoints() == metadata2.getNumOfPoints());
      assertTrue(metadata1.getStartTime() == metadata2.getStartTime());
      assertTrue(metadata1.getEndTime() == metadata2.getEndTime());
      assertNotNull(metadata1.getStatistics());
      assertNotNull(metadata2.getStatistics());
      Utils.isTwoTsDigestEqual(metadata1.getStatistics(), metadata2.getStatistics(), "TsDigest");
    }
  }

  public static boolean isFileMetaDataEqual(TsFileMetadata metadata1, TsFileMetadata metadata2) {
    if (Utils.isTwoObjectsNotNULL(metadata1, metadata2, "File MetaData")) {
      if (Utils.isTwoObjectsNotNULL(metadata1.getMetadataIndex(), metadata2.getMetadataIndex(),
          "Metadata Index")) {
        MetadataIndexNode metaDataIndex1 = metadata1.getMetadataIndex();
        MetadataIndexNode metaDataIndex2 = metadata2.getMetadataIndex();
        return metaDataIndex1.getChildren().size() == metaDataIndex2.getChildren().size();
      }
    }
    return false;
  }

  public static void isPageHeaderEqual(PageHeader header1, PageHeader header2) {
    if (Utils.isTwoObjectsNotNULL(header1, header2, "PageHeader")) {
      assertEquals(header1.getUncompressedSize(), header2.getUncompressedSize());
      assertEquals(header1.getCompressedSize(), header2.getCompressedSize());
      assertEquals(header1.getNumOfValues(), header2.getNumOfValues());
      assertEquals(header1.getEndTime(), header2.getEndTime());
      assertEquals(header1.getStartTime(), header2.getStartTime());
      if (Utils
          .isTwoObjectsNotNULL(header1.getStatistics(), header2.getStatistics(), "statistics")) {
        Utils.isStatisticsEqual(header1.getStatistics(), header2.getStatistics());
      }
    }
  }

  public static void isStatisticsEqual(Statistics statistics1, Statistics statistics2) {
    if ((statistics1 == null) || (statistics2 == null)) {
      System.out.println("error");
      fail("one of statistics is null");
    }
    if (statistics1.isEmpty() || statistics2.isEmpty()) {
      fail("one of statistics is empty while the other one is not");
    }
    if (!statistics1.isEmpty() && !statistics2.isEmpty()) {
      assertEquals(statistics1.getMinValue(), statistics2.getMinValue());
      assertEquals(statistics1.getMaxValue(), statistics2.getMaxValue());
      assertEquals(statistics1.getFirstValue(), statistics2.getFirstValue());
      if (statistics1 instanceof IntegerStatistics || statistics1 instanceof BooleanStatistics) {
        assertEquals(statistics1.getSumLongValue(), statistics2.getSumLongValue());
      } else {
        assertEquals(statistics1.getSumDoubleValue(), statistics2.getSumDoubleValue(), maxError);
      }
      assertEquals(statistics1.getLastValue(), statistics2.getLastValue());
    }
  }
}