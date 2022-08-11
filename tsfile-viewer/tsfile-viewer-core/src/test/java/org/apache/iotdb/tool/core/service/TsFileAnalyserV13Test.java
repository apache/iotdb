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
