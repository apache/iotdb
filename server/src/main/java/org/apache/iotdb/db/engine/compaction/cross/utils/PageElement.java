package org.apache.iotdb.db.engine.compaction.cross.utils;

import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

import java.nio.ByteBuffer;

public class PageElement {
  public PageHeader pageHeader;

  public ByteBuffer pageData;

  public ChunkReader chunkReader;

  public int priority;

  public long startTime;

  public boolean isOverlaped = false;

  public PageElement(
      PageHeader pageHeader, ByteBuffer pageData, ChunkReader chunkReader, int priority) {
    this.pageHeader = pageHeader;
    this.pageData = pageData;
    this.priority = priority;
    this.chunkReader = chunkReader;
    this.startTime = pageHeader.getStartTime();
  }
}
