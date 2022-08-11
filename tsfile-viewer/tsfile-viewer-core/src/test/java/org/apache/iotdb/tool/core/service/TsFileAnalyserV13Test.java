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

package org.apache.iotdb.tool.core.service;

import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TsFileAnalyserV13Test {

  @Test
  public void testTsFileAnalyserV13ForAligned() throws IOException, InterruptedException {
    String filePath = "alignedRecord.tsfile";
    TsFileAnalyserV13 tsFileAnalyserV13 = new TsFileAnalyserV13(filePath);
    tsFileAnalyserV13.getTimeSeriesMetadataNode();
    // 是否有序
    assertTrue(tsFileAnalyserV13.isSeq());
    // 进度条完成
    assertEquals(1.0, tsFileAnalyserV13.getRateOfProcess(), 0);
    // 是否AlignedChunkMetadata
    assertTrue(
        tsFileAnalyserV13.getChunkGroupMetadataModelList().get(0).getChunkMetadataList().get(0)
            instanceof AlignedChunkMetadata);
    // 判断node节点
    assertTrue(tsFileAnalyserV13.getTimeSeriesMetadataNode().getChildren().size() > 0);
  }

  @Test
  public void testTsFileAnalyserV13ForNonAligned() throws IOException, InterruptedException {
    String filePath = "test.tsfile";
    TsFileAnalyserV13 tsFileAnalyserV13 = new TsFileAnalyserV13(filePath);
    tsFileAnalyserV13.getTimeSeriesMetadataNode();
    // 是否有序
    assertTrue(tsFileAnalyserV13.isSeq());
    // 进度条完成
    assertEquals(1.0, tsFileAnalyserV13.getRateOfProcess(), 0);
    // 是否AlignedChunkMetadata
    assertTrue(
        tsFileAnalyserV13.getChunkGroupMetadataModelList().get(0).getChunkMetadataList().get(0)
            instanceof ChunkMetadata);
    // 判断node节点
    assertTrue(tsFileAnalyserV13.getTimeSeriesMetadataNode().getChildren().size() > 0);
  }
}
