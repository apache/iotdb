package org.apache.iotdb.db.engine.compaction.cross.utils;

import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.AlignedChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class PageElement {

  public PageHeader pageHeader;

  public List<PageHeader> valuePageHeaders;

  public TsBlock batchData;

  // compressed page data
  public ByteBuffer pageData;

  public List<ByteBuffer> valuePageDatas;

  public IChunkReader iChunkReader;

  public int priority;

  public long startTime;

  public boolean isOverlaped = false;

  public boolean isLastPage;

  public ChunkMetadataElement chunkMetadataElement;

  public PageElement(
      PageHeader pageHeader,
      ByteBuffer pageData,
      ChunkReader iChunkReader,
      ChunkMetadataElement chunkMetadataElement,
      boolean isLastPage,
      int priority) {
    this.pageHeader = pageHeader;
    this.pageData = pageData;
    this.priority = priority;
    this.iChunkReader = iChunkReader;
    this.startTime = pageHeader.getStartTime();
    this.chunkMetadataElement = chunkMetadataElement;
    this.isLastPage = isLastPage;
  }

  public PageElement(
      PageHeader timePageHeader,
      List<PageHeader> valuePageHeaders,
      ByteBuffer timePageData,
      List<ByteBuffer> valuePageDatas,
      ChunkReader iChunkReader,
      ChunkMetadataElement timeChunkMetadataElement,
      boolean isLastPage,
      int priority) {
    this.pageHeader = timePageHeader;
    this.valuePageHeaders = valuePageHeaders;
    this.pageData = timePageData;
    this.valuePageDatas = valuePageDatas;
    this.priority = priority;
    this.iChunkReader = iChunkReader;
    this.startTime = pageHeader.getStartTime();
    this.chunkMetadataElement = timeChunkMetadataElement;
    this.isLastPage = isLastPage;
  }

  public void deserializePage() throws IOException {
    if (iChunkReader instanceof AlignedChunkReader) {
      this.batchData =
          ((AlignedChunkReader) iChunkReader)
              .readPageData(pageHeader, valuePageHeaders, pageData, valuePageDatas);
    } else {
      this.batchData = ((ChunkReader) iChunkReader).readPageData(pageHeader, pageData);
    }
  }
}
