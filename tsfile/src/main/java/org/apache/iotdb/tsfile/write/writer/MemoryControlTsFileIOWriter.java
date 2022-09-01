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

package org.apache.iotdb.tsfile.write.writer;

import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.LocalTsFileInput;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MemoryControlTsFileIOWriter extends TsFileIOWriter {
  private static final Logger LOG = LoggerFactory.getLogger(MemoryControlTsFileIOWriter.class);
  private long maxMetadataSize;
  private long currentChunkMetadataSize = 0L;
  private File chunkMetadataTempFile;
  protected LocalTsFileOutput tempOutput;
  protected LocalTsFileInput tempInput;
  private final boolean needSort;
  private List<Long> sortedSegmentPosition = new ArrayList<>();
  private ByteBuffer typeBuffer = ByteBuffer.allocate(1);
  private ByteBuffer sizeBuffer = ByteBuffer.allocate(4);

  public static final String CHUNK_METADATA_TEMP_FILE_PREFIX = ".cmt";
  private static final byte VECTOR_TYPE = 1;
  private static final byte NORMAL_TYPE = 2;

  public MemoryControlTsFileIOWriter(File file, long maxMetadataSize, boolean needSort)
      throws IOException {
    super(file);
    this.maxMetadataSize = maxMetadataSize;
    this.chunkMetadataTempFile = new File(file.getAbsoluteFile() + CHUNK_METADATA_TEMP_FILE_PREFIX);
    this.needSort = needSort;
  }

  @Override
  public void endCurrentChunk() {
    currentChunkMetadataSize += currentChunkMetadata.calculateRamSize();
    super.endCurrentChunk();
    if (currentChunkMetadataSize > maxMetadataSize) {
      // TODO: Sort and flush the chunk metadata to outside
      try {
        sortAndFlushChunkMetadata();
      } catch (IOException e) {
        LOG.error("Meets exception when flushing metadata to temp files", e);
      }
    }
  }

  protected void sortAndFlushChunkMetadata() throws IOException {
    // group by series
    Map<Path, List<IChunkMetadata>> chunkMetadataListMap = groupChunkMetadataListBySeries();
    if (tempOutput == null) {
      tempOutput = new LocalTsFileOutput(new FileOutputStream(chunkMetadataTempFile));
    }
    sortedSegmentPosition.add(tempOutput.getPosition());
    // the file structure in temp file will be
    // ChunkType | chunkSize | chunkBuffer
    for (Map.Entry<Path, List<IChunkMetadata>> entry : chunkMetadataListMap.entrySet()) {
      Path seriesPath = entry.getKey();
      List<IChunkMetadata> iChunkMetadataList = entry.getValue();
      if (iChunkMetadataList.size() > 0
          && iChunkMetadataList.get(0).getDataType() == TSDataType.VECTOR) {
        // this is a vector chunk
        writeAlignedChunkMetadata(iChunkMetadataList, seriesPath);
      } else {
        writeNormalChunkMetadata(iChunkMetadataList, seriesPath);
      }
    }
  }

  private void writeAlignedChunkMetadata(List<IChunkMetadata> iChunkMetadataList, Path seriesPath)
      throws IOException {
    ReadWriteIOUtils.write(VECTOR_TYPE, tempOutput);
    IChunkMetadata currentTimeChunk = iChunkMetadataList.get(0);
    List<IChunkMetadata> currentValueChunk = new ArrayList<>();
    List<AlignedChunkMetadata> alignedChunkMetadata = new ArrayList<>();
    for (int i = 1; i < iChunkMetadataList.size(); ++i) {
      if (iChunkMetadataList.get(i).getDataType() == TSDataType.VECTOR) {
        alignedChunkMetadata.add(new AlignedChunkMetadata(currentTimeChunk, currentValueChunk));
        currentTimeChunk = iChunkMetadataList.get(i);
        currentValueChunk = new ArrayList<>();
      } else {
        currentValueChunk.add(iChunkMetadataList.get(i));
      }
    }
    if (currentValueChunk.size() > 0) {
      alignedChunkMetadata.add(new AlignedChunkMetadata(currentTimeChunk, currentValueChunk));
    }
    for (IChunkMetadata chunkMetadata : alignedChunkMetadata) {
      PublicBAOS buffer = new PublicBAOS();
      int size = chunkMetadata.serializeWithFullInfo(buffer, seriesPath.getDevice());
      ReadWriteIOUtils.write(size, tempOutput);
      buffer.writeTo(tempOutput);
    }
  }

  private void writeNormalChunkMetadata(List<IChunkMetadata> iChunkMetadataList, Path seriesPath)
      throws IOException {
    ReadWriteIOUtils.write(NORMAL_TYPE, tempOutput);
    for (IChunkMetadata chunkMetadata : iChunkMetadataList) {
      PublicBAOS buffer = new PublicBAOS();
      int size = chunkMetadata.serializeWithFullInfo(buffer, seriesPath.getFullPath());
      ReadWriteIOUtils.write(size, tempOutput);
      buffer.writeTo(tempOutput);
    }
  }

  protected Pair<String, IChunkMetadata> readNextChunkMetadata() throws IOException {
    if (tempInput == null) {
      tempInput = new LocalTsFileInput(chunkMetadataTempFile.toPath());
    }
    byte type = readNextChunkMetadataType();
    int size = readNextChunkMetadataSize();
    ByteBuffer chunkBuffer = ByteBuffer.allocate(size);
    ReadWriteIOUtils.readAsPossible(tempInput, chunkBuffer);
    chunkBuffer.flip();
    if (type == NORMAL_TYPE) {
      ChunkMetadata chunkMetadata = new ChunkMetadata();
      String seriesPath = ChunkMetadata.deserializeWithFullInfo(chunkBuffer, chunkMetadata);
      return new Pair<>(seriesPath, chunkMetadata);
    } else {
      AlignedChunkMetadata chunkMetadata = new AlignedChunkMetadata();
      String devicePath = AlignedChunkMetadata.deserializeWithFullInfo(chunkBuffer, chunkMetadata);
      return new Pair<>(devicePath, chunkMetadata);
    }
  }

  private byte readNextChunkMetadataType() throws IOException {
    typeBuffer.clear();
    ReadWriteIOUtils.readAsPossible(tempInput, typeBuffer);
    typeBuffer.flip();
    return ReadWriteIOUtils.readByte(typeBuffer);
  }

  private int readNextChunkMetadataSize() throws IOException {
    sizeBuffer.clear();
    ReadWriteIOUtils.readAsPossible(tempInput, sizeBuffer);
    sizeBuffer.flip();
    return ReadWriteIOUtils.readInt(sizeBuffer);
  }

  @Override
  public void endFile() {
    //		super.endFile();
  }

  @Override
  public void close() throws IOException {
    super.close();
    if (tempInput != null) {
      tempInput.close();
    }
    if (tempOutput != null) {
      this.tempOutput.close();
    }
  }
}
