package org.apache.iotdb.db.engine.compaction.cross.utils;

import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

import java.nio.ByteBuffer;

public class PageElement {
  public PageHeader pageHeader;

  public BatchData batchData;

  // compressed page data
  public ByteBuffer pageData;

  public ChunkReader chunkReader;

  public int priority;

  public long startTime;

  public boolean isOverlaped = false;

  public boolean isLastPage;

  public ChunkMetadataElement chunkMetadataElement;

  public PageElement(
      PageHeader pageHeader,
      ByteBuffer pageData,
      ChunkReader chunkReader,
      ChunkMetadataElement chunkMetadataElement,
      boolean isLastPage,
      int priority) {
    this.pageHeader = pageHeader;
    this.pageData = pageData;
    this.priority = priority;
    this.chunkReader = chunkReader;
    this.startTime = pageHeader.getStartTime();
    this.chunkMetadataElement = chunkMetadataElement;
    this.isLastPage = isLastPage;
  }
}
