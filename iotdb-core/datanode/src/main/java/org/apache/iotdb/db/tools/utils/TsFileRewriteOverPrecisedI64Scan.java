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
package org.apache.iotdb.db.tools.utils;

import org.apache.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.reader.IPageReader;
import org.apache.tsfile.read.reader.chunk.ChunkReader;
import org.apache.tsfile.read.reader.chunk.TableChunkReader;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.chunk.ValueChunkWriter;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class TsFileRewriteOverPrecisedI64Scan extends TsFileSequenceScan {

  private final File target;
  private TsFileIOWriter writer;
  private Chunk currTimeChunk;

  public TsFileRewriteOverPrecisedI64Scan(File target) {
    this.target = target;
  }

  public static void main(String[] args) {
    File sourceFile = new File(args[0]);
    File targetFile = new File(args[1]);
    TsFileRewriteOverPrecisedI64Scan scan = new TsFileRewriteOverPrecisedI64Scan(targetFile);
    scan.scanTsFile(sourceFile);

    long sourceLength = sourceFile.length();
    long targetLength = targetFile.length();
    System.out.printf(
        "Before rewrite %d, after rewrite %d, ratio %f%n",
        sourceLength, targetLength, sourceLength * 1.0 / targetLength);
  }

  @Override
  protected boolean onFileOpen(File file) throws IOException {
    boolean shouldScan = super.onFileOpen(file);
    if (shouldScan) {
      writer = new TsFileIOWriter(target);
    }
    return shouldScan;
  }

  @Override
  protected void onFileEnd() throws IOException {
    writer.endChunkGroup();
    writer.endFile();
  }

  @Override
  protected void onChunkGroup() throws IOException {
    if (currDeviceID != null) {
      writer.endChunkGroup();
    }
    super.onChunkGroup();
    writer.startChunkGroup(currDeviceID);
  }

  @Override
  protected void onChunk(PageVisitor pageVisitor) throws IOException {
    reader.position(reader.position() - 1);
    Chunk chunk = reader.readMemChunk(reader.position());
    chunk =
        new Chunk(
            chunk.getHeader(),
            chunk.getData(),
            Collections.emptyList(),
            Statistics.getStatsByType(chunk.getHeader().getDataType()));
    currMeasurementID = chunk.getHeader().getMeasurementID();
    currTimeseriesID = new Pair<>(currDeviceID, currMeasurementID);
    if (!currDeviceAligned) {
      onNonAlignedChunk(chunk);
    } else {
      onAlignedChunk(chunk);
    }
    System.out.println("Processed a chunk of " + currDeviceID + "." + currMeasurementID);
    reader.position(
        reader.position()
            + chunk.getHeader().getSerializedSize()
            + chunk.getHeader().getDataSize());
  }

  private void onNonAlignedChunk(Chunk chunk) throws IOException {
    if (chunk.getHeader().getDataType() != TSDataType.INT64) {
      writer.writeChunk(chunk);
    } else {
      if (!rewriteInt64ChunkNonAligned(chunk)) {
        writer.writeChunk(chunk);
      }
    }
  }

  private void onAlignedChunk(Chunk chunk) throws IOException {
    if (isTimeChunk || chunk.getHeader().getDataType() != TSDataType.INT64) {
      writer.writeChunk(chunk);
      if (isTimeChunk) {
        currTimeChunk = chunk;
      }
    } else {
      if (!rewriteInt64ChunkAligned(chunk)) {
        writer.writeChunk(chunk);
      }
    }
  }

  private boolean rewriteInt64ChunkNonAligned(Chunk chunk) throws IOException {
    ChunkReader chunkReader = new ChunkReader(chunk);
    ChunkHeader header = chunk.getHeader();
    List<IPageReader> pageReaders = chunkReader.loadPageReaderList();
    ChunkWriterImpl chunkWriter =
        new ChunkWriterImpl(
            new MeasurementSchema(
                header.getMeasurementID(),
                TSDataType.INT32,
                header.getEncodingType(),
                header.getCompressionType()));

    for (IPageReader pageReader : pageReaders) {
      BatchData batchData = pageReader.getAllSatisfiedPageData();
      while (batchData.hasCurrent()) {
        int intVal = (int) batchData.getLong();
        if (intVal != batchData.getLong()) {
          // the chunk is not over-precised
          return false;
        }
        chunkWriter.write(batchData.currentTime(), (int) batchData.getLong());
      }
      chunkWriter.sealCurrentPage();
    }
    chunkWriter.writeToFileWriter(writer);
    return true;
  }

  private boolean rewriteInt64ChunkAligned(Chunk chunk) throws IOException {
    // use TableChunkReader so that nulls will not be skipped
    TableChunkReader chunkReader =
        new TableChunkReader(currTimeChunk, Collections.singletonList(chunk), null);
    ChunkHeader header = chunk.getHeader();
    List<IPageReader> pageReaders = chunkReader.loadPageReaderList();
    ValueChunkWriter valueChunkWriter =
        new ValueChunkWriter(
            header.getMeasurementID(),
            header.getCompressionType(),
            TSDataType.INT32,
            header.getEncodingType(),
            TSEncodingBuilder.getEncodingBuilder(header.getEncodingType())
                .getEncoder(TSDataType.INT32));

    for (IPageReader pageReader : pageReaders) {
      BatchData batchData = pageReader.getAllSatisfiedPageData();
      while (batchData.hasCurrent()) {
        TsPrimitiveType[] vector = batchData.getVector();
        boolean isNull = vector[0] == null;
        if (!isNull) {
          int intVal = (int) vector[0].getLong();
          if (intVal != vector[0].getLong()) {
            // the chunk is not over-precised
            return false;
          }
          valueChunkWriter.write(batchData.currentTime(), intVal, false);
        } else {
          valueChunkWriter.write(batchData.currentTime(), 0, true);
        }
      }
      valueChunkWriter.sealCurrentPage();
    }
    valueChunkWriter.writeToFileWriter(writer);
    return true;
  }

  @Override
  protected void onTimePage(PageHeader pageHeader, ByteBuffer pageData, ChunkHeader chunkHeader) {
    // do nothing
  }

  @Override
  protected void onValuePage(PageHeader pageHeader, ByteBuffer pageData, ChunkHeader chunkHeader) {
    // do nothing
  }

  @Override
  protected void onNonAlignedPage(
      PageHeader pageHeader, ByteBuffer pageData, ChunkHeader chunkHeader) {
    // do nothing
  }

  @Override
  protected void onException(Throwable t) {
    throw new RuntimeException(t);
  }
}
