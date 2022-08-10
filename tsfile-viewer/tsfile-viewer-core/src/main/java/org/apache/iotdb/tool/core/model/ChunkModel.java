package org.apache.iotdb.tool.core.model;

import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;

import java.util.List;

public class ChunkModel {

  private ChunkMetadata chunkMetadata;

  private List<PageHeader> pageHeaders;

  private List<BatchData> batchDataList;

  private Chunk chunk;

  public ChunkMetadata getChunkMetadata() {
    return chunkMetadata;
  }

  public void setChunkMetadata(ChunkMetadata chunkMetadata) {
    this.chunkMetadata = chunkMetadata;
  }

  public List<PageHeader> getPageHeaders() {
    return pageHeaders;
  }

  public void setPageHeaders(List<PageHeader> pageHeaders) {
    this.pageHeaders = pageHeaders;
  }

  public List<BatchData> getBatchDataList() {
    return batchDataList;
  }

  public void setBatchDataList(List<BatchData> batchDataList) {
    this.batchDataList = batchDataList;
  }

  public Chunk getChunk() {
    return chunk;
  }

  public void setChunk(Chunk chunk) {
    this.chunk = chunk;
  }
}
