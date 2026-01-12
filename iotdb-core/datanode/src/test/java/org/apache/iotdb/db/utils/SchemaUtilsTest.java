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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.utils.constant.SqlConstant;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.AbstractAlignedChunkMetadata;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.utils.Binary;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.tsfile.file.metadata.statistics.Statistics.canMerge;

public class SchemaUtilsTest {
  @Test
  public void getAggregatedDataTypesTest() {
    List<TSDataType> measurementTypes = new ArrayList<>();
    measurementTypes.add(TSDataType.INT64);
    measurementTypes.add(TSDataType.TEXT);
    measurementTypes.add(TSDataType.BOOLEAN);
    measurementTypes.add(TSDataType.DOUBLE);
    Assert.assertEquals(
        Collections.nCopies(measurementTypes.size(), TSDataType.INT64),
        SchemaUtils.getAggregatedDataTypes(measurementTypes, SqlConstant.MIN_TIME));
    Assert.assertEquals(
        Collections.nCopies(measurementTypes.size(), TSDataType.INT64),
        SchemaUtils.getAggregatedDataTypes(measurementTypes, SqlConstant.COUNT));
    Assert.assertEquals(
        Collections.nCopies(measurementTypes.size(), TSDataType.DOUBLE),
        SchemaUtils.getAggregatedDataTypes(measurementTypes, SqlConstant.SUM));
    Assert.assertEquals(
        measurementTypes,
        SchemaUtils.getAggregatedDataTypes(measurementTypes, SqlConstant.LAST_VALUE));
    Assert.assertEquals(
        measurementTypes,
        SchemaUtils.getAggregatedDataTypes(measurementTypes, SqlConstant.MAX_VALUE));
  }

  @Test
  public void getSeriesTypeByPath() {
    Assert.assertEquals(
        TSDataType.DOUBLE, SchemaUtils.getSeriesTypeByPath(TSDataType.INT64, SqlConstant.SUM));
    Assert.assertEquals(
        TSDataType.INT64,
        SchemaUtils.getSeriesTypeByPath(TSDataType.INT64, SqlConstant.LAST_VALUE));
  }

  @Test
  public void checkDataTypeWithEncoding() {
    try {
      SchemaUtils.checkDataTypeWithEncoding(TSDataType.TEXT, TSEncoding.RLE);
      Assert.fail("expect exception");
    } catch (MetadataException e) {
      // do nothing
    }
  }

  @Test
  public void rewriteAlignedChunkMetadataStatistics() {
    for (TSDataType targetDataType : Arrays.asList(TSDataType.STRING, TSDataType.TEXT)) {
      for (TSDataType tsDataType : TSDataType.values()) {
        if (tsDataType == TSDataType.UNKNOWN
            || tsDataType == TSDataType.VECTOR
            || tsDataType == TSDataType.OBJECT) {
          continue;
        }
        List<IChunkMetadata> valueChunkMetadatas =
            Collections.singletonList(
                new ChunkMetadata(
                    "s0",
                    tsDataType,
                    TSEncoding.RLE,
                    CompressionType.LZ4,
                    0,
                    Statistics.getStatsByType(tsDataType)));
        AlignedChunkMetadata alignedChunkMetadata =
            new AlignedChunkMetadata(new ChunkMetadata(), valueChunkMetadatas);
        try {
          SchemaUtils.rewriteAlignedChunkMetadataStatistics(
              alignedChunkMetadata, 0, targetDataType);
          if (alignedChunkMetadata != null
              && !alignedChunkMetadata.getValueChunkMetadataList().isEmpty()) {
            Assert.assertEquals(
                targetDataType,
                alignedChunkMetadata.getValueChunkMetadataList().get(0).getDataType());
          }
        } catch (ClassCastException e) {
          Assert.fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void mergeMetadataStatistics() throws Exception {
    Set<TSDataType> unsupportTsDataType = new HashSet<>();
    unsupportTsDataType.add(TSDataType.UNKNOWN);
    unsupportTsDataType.add(TSDataType.VECTOR);
    for (TSDataType sourceDataType : Arrays.asList(TSDataType.DOUBLE)) {
      for (TSDataType targetDataType : Arrays.asList(TSDataType.TEXT, TSDataType.BLOB)) {

        if (sourceDataType.equals(targetDataType)) {
          continue;
        }
        if (unsupportTsDataType.contains(sourceDataType)
            || unsupportTsDataType.contains(targetDataType)) {
          continue;
        }

        System.out.println("from " + sourceDataType + " to " + targetDataType);

        // Aligned series
        Statistics<?> s1 = Statistics.getStatsByType(sourceDataType);
        s1.update(new long[] {1, 2}, new double[] {1.0, 2.0}, 2);
        Statistics<?> s2 = Statistics.getStatsByType(TSDataType.DOUBLE);
        s2.update(new long[] {1, 2}, new double[] {1.0, 2.0}, 2);
        List<IChunkMetadata> valueChunkMetadatas =
            Arrays.asList(
                new ChunkMetadata(
                    "s0",
                    sourceDataType,
                    SchemaUtils.getDataTypeCompatibleEncoding(sourceDataType, TSEncoding.RLE),
                    CompressionType.LZ4,
                    0,
                    s1),
                new ChunkMetadata(
                    "s1",
                    TSDataType.DOUBLE,
                    SchemaUtils.getDataTypeCompatibleEncoding(TSDataType.DOUBLE, TSEncoding.RLE),
                    CompressionType.LZ4,
                    0,
                    s2));
        IChunkMetadata alignedChunkMetadata =
            new AlignedChunkMetadata(new ChunkMetadata(), valueChunkMetadatas);

        Statistics<?> s3 = Statistics.getStatsByType(targetDataType);
        if (targetDataType == TSDataType.BLOB) {
          s3.update(3, new Binary("3", StandardCharsets.UTF_8));
          s3.update(4, new Binary("4", StandardCharsets.UTF_8));
        } else {
          s3.update(
              new long[] {1, 2},
              new Binary[] {
                new Binary("3", StandardCharsets.UTF_8), new Binary("4", StandardCharsets.UTF_8),
              },
              2);
        }
        Statistics<?> s4 = Statistics.getStatsByType(targetDataType);
        if (targetDataType == TSDataType.BLOB) {
          s3.update(4, new Binary("4", StandardCharsets.UTF_8));
        } else {
          s4.update(
              new long[] {1, 2},
              new Binary[] {
                new Binary("5", StandardCharsets.UTF_8), new Binary("6", StandardCharsets.UTF_8),
              },
              2);
        }
        List<IChunkMetadata> targetChunkMetadatas =
            Arrays.asList(
                new ChunkMetadata(
                    "s0",
                    targetDataType,
                    SchemaUtils.getDataTypeCompatibleEncoding(targetDataType, TSEncoding.RLE),
                    CompressionType.LZ4,
                    0,
                    s3),
                new ChunkMetadata(
                    "s1",
                    targetDataType,
                    SchemaUtils.getDataTypeCompatibleEncoding(targetDataType, TSEncoding.RLE),
                    CompressionType.LZ4,
                    0,
                    s4));
        AbstractAlignedChunkMetadata abstractAlignedChunkMetadata =
            (AbstractAlignedChunkMetadata) alignedChunkMetadata;
        try {
          for (int i = 0; i < 2; i++) {
            SchemaUtils.rewriteAlignedChunkMetadataStatistics(
                abstractAlignedChunkMetadata, i, targetDataType);
          }
        } catch (ClassCastException e) {
          Assert.fail(e.getMessage());
        }

        for (int i = 0; i < targetChunkMetadatas.size(); i++) {
          if (!abstractAlignedChunkMetadata.getValueChunkMetadataList().isEmpty()
              && abstractAlignedChunkMetadata.getValueChunkMetadataList().get(i) != null) {
            if (targetChunkMetadatas.get(i).getStatistics().getClass()
                    == abstractAlignedChunkMetadata
                        .getValueChunkMetadataList()
                        .get(i)
                        .getStatistics()
                        .getClass()
                || canMerge(
                    abstractAlignedChunkMetadata
                        .getValueChunkMetadataList()
                        .get(i)
                        .getStatistics()
                        .getType(),
                    targetChunkMetadatas.get(i).getStatistics().getType())) {
              targetChunkMetadatas
                  .get(i)
                  .getStatistics()
                  .mergeStatistics(
                      abstractAlignedChunkMetadata
                          .getValueChunkMetadataList()
                          .get(i)
                          .getStatistics());
            } else {
              throw new Exception("unsupported");
            }
          }
        }
      }
    }
  }
}
