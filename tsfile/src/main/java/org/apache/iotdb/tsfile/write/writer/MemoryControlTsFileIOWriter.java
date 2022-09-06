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

import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class MemoryControlTsFileIOWriter extends TsFileIOWriter {
  private static final Logger LOG = LoggerFactory.getLogger(MemoryControlTsFileIOWriter.class);
  protected long maxMetadataSize;
  protected long currentChunkMetadataSize = 0L;
  protected File chunkMetadataTempFile;
  protected LocalTsFileOutput tempOutput;
  protected final boolean autoControl;
  // it stores the start address of persisted chunk metadata for per series
  protected Queue<Long> segmentForPerSeries = new ArrayDeque<>();
  protected String currentSeries = null;

  public static final String CHUNK_METADATA_TEMP_FILE_PREFIX = ".cmt";
  private static final byte VECTOR_TYPE = 1;
  private static final byte NORMAL_TYPE = 2;

  public MemoryControlTsFileIOWriter(File file, long maxMetadataSize, boolean autoControl)
      throws IOException {
    super(file);
    this.maxMetadataSize = maxMetadataSize;
    this.chunkMetadataTempFile = new File(file.getAbsoluteFile() + CHUNK_METADATA_TEMP_FILE_PREFIX);
    this.autoControl = autoControl;
  }

  @Override
  public void endCurrentChunk() {
    currentChunkMetadataSize += currentChunkMetadata.calculateRamSize();
    super.endCurrentChunk();
    if (this.autoControl) {
      checkMetadataSizeAndMayFlush();
    }
  }

  public boolean checkMetadataSizeAndMayFlush() {
    if (currentChunkMetadataSize > maxMetadataSize) {
      try {
        sortAndFlushChunkMetadata();
        return true;
      } catch (IOException e) {
        LOG.error("Meets exception when flushing metadata to temp files", e);
      }
    }
    return false;
  }

  protected void sortAndFlushChunkMetadata() throws IOException {
    // group by series
    Map<Path, List<IChunkMetadata>> chunkMetadataListMap = groupChunkMetadataListBySeries();
    if (tempOutput == null) {
      tempOutput = new LocalTsFileOutput(new FileOutputStream(chunkMetadataTempFile));
    }
    // the file structure in temp file will be
    // ChunkType | chunkSize | chunkBuffer
    for (Map.Entry<Path, List<IChunkMetadata>> entry : chunkMetadataListMap.entrySet()) {
      segmentForPerSeries.add(tempOutput.getPosition());
      Path seriesPath = entry.getKey();
      List<IChunkMetadata> iChunkMetadataList = entry.getValue();
      writeChunkMetadata(iChunkMetadataList, seriesPath, tempOutput);
    }
  }

  private void writeChunkMetadata(
      List<IChunkMetadata> iChunkMetadataList, Path seriesPath, LocalTsFileOutput output)
      throws IOException {
    if (iChunkMetadataList.size() == 0) {
      return;
    }
    if (iChunkMetadataList.get(0).getDataType() == TSDataType.VECTOR) {
      // pack the TimeChunkMetadata and List<ValueChunkMetadata> into List<AlignedChunkMetadata>
      List<IChunkMetadata> alignedChunkMetadata = packAlignedChunkMetadata(iChunkMetadataList);
      writeAlignedChunkMetadata(alignedChunkMetadata, seriesPath, output);
    } else {
      writeNormalChunkMetadata(iChunkMetadataList, seriesPath, output);
    }
  }

  private List<IChunkMetadata> packAlignedChunkMetadata(List<IChunkMetadata> iChunkMetadataList) {
    IChunkMetadata currentTimeChunk = iChunkMetadataList.get(0);
    List<IChunkMetadata> currentValueChunk = new ArrayList<>();
    List<IChunkMetadata> alignedChunkMetadata = new ArrayList<>();
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
    return alignedChunkMetadata;
  }

  private void writeAlignedChunkMetadata(
      List<IChunkMetadata> iChunkMetadataList, Path seriesPath, LocalTsFileOutput output)
      throws IOException {
    ReadWriteIOUtils.write(VECTOR_TYPE, output);
    for (IChunkMetadata chunkMetadata : iChunkMetadataList) {
      PublicBAOS buffer = new PublicBAOS();
      int size = chunkMetadata.serializeWithFullInfo(buffer, seriesPath.getDevice());
      ReadWriteIOUtils.write(size, output);
      buffer.writeTo(output);
    }
  }

  private void writeNormalChunkMetadata(
      List<IChunkMetadata> iChunkMetadataList, Path seriesPath, LocalTsFileOutput output)
      throws IOException {
    ReadWriteIOUtils.write(NORMAL_TYPE, output);
    for (IChunkMetadata chunkMetadata : iChunkMetadataList) {
      PublicBAOS buffer = new PublicBAOS();
      int size = chunkMetadata.serializeWithFullInfo(buffer, seriesPath.getFullPath());
      ReadWriteIOUtils.write(size, output);
      buffer.writeTo(output);
    }
  }

  @Override
  public void endFile() throws IOException {
    if (this.segmentForPerSeries.size() > 0) {
      // there is some chunk metadata already been written to the disk
      // first we should flush the remaining chunk metadata in memory to disk
      // then read the persisted chunk metadata from disk
      sortAndFlushChunkMetadata();
      tempOutput.close();
    } else {
      // sort the chunk metadata in memory, construct the index tree
      // and just close the file
      tempOutput.close();
      super.endFile();
      return;
    }

    // read in the chunk metadata, and construct the index tree
    readChunkMetadataAndConstructIndexTree();
  }

  private void readChunkMetadataAndConstructIndexTree() throws IOException {
    tempOutput.close();
    long metaOffset = out.getPosition();

    // serialize the SEPARATOR of MetaData
    ReadWriteIOUtils.write(MetaMarker.SEPARATOR, out.wrapAsStream());
    ChunkMetadataReadIterator iterator =
        new ChunkMetadataReadIterator(
            0,
            chunkMetadataTempFile.length(),
            new LocalTsFileInput(chunkMetadataTempFile.toPath()));
    while (iterator.hasNextChunkMetadata()) {
      // 1. read in all chunk metadata of one series
      // 2. construct the timeseries metadata for this series
      // 3. construct the index tree node for the series
      // 4. serialize the timeseries metadata to file
      TimeseriesMetadata timeseriesMetadata = readTimeseriesMetadata(iterator);
    }
  }

  private TimeseriesMetadata readTimeseriesMetadata(ChunkMetadataReadIterator iterator)
      throws IOException {
    Pair<String, IChunkMetadata> currentPair = iterator.getCurrentPair();
    if (currentPair == null) {
      currentPair = iterator.getNextSeriesNameAndChunkMetadata();
    }
    if (!currentPair.left.equals(currentSeries)) {
      // come to a new series
      currentSeries = currentPair.left;
    }
    List<IChunkMetadata> iChunkMetadataList = new ArrayList<>();
    while (currentPair != null && currentPair.left.equals(currentSeries)) {
      iChunkMetadataList.add(currentPair.right);
      currentPair = iterator.getNextSeriesNameAndChunkMetadata();
    }
    return super.constructOneTimeseriesMetadata(new Path(currentSeries), iChunkMetadataList, false);
  }

  @Override
  public void close() throws IOException {
    super.close();
    if (tempOutput != null) {
      this.tempOutput.close();
    }
  }

  protected class ChunkMetadataReadIterator {

    final LocalTsFileInput input;
    final long startPosition;
    final long endPosition;
    final ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
    final ByteBuffer typeBuffer = ByteBuffer.allocate(1);
    private Pair<String, IChunkMetadata> currentPair = null;

    ChunkMetadataReadIterator(long startPosition, long endPosition, LocalTsFileInput input)
        throws IOException {
      this.startPosition = startPosition;
      this.endPosition = endPosition;
      this.input = input;
      this.input.position(startPosition);
    }

    public boolean hasNextChunkMetadata() throws IOException {
      return currentPair != null || this.input.position() < endPosition;
    }

    public Pair<String, IChunkMetadata> getNextSeriesNameAndChunkMetadata() throws IOException {
      if (input.position() >= endPosition) {
        currentPair = null;
        return null;
      }
      byte type = readNextChunkMetadataType();
      int size = readNextChunkMetadataSize();
      ByteBuffer chunkBuffer = ByteBuffer.allocate(size);
      ReadWriteIOUtils.readAsPossible(input, chunkBuffer);
      chunkBuffer.flip();
      if (type == NORMAL_TYPE) {
        ChunkMetadata chunkMetadata = new ChunkMetadata();
        String seriesPath = ChunkMetadata.deserializeWithFullInfo(chunkBuffer, chunkMetadata);
        currentPair = new Pair<>(seriesPath, chunkMetadata);
      } else {
        AlignedChunkMetadata chunkMetadata = new AlignedChunkMetadata();
        String devicePath =
            AlignedChunkMetadata.deserializeWithFullInfo(chunkBuffer, chunkMetadata);
        currentPair = new Pair<>(devicePath, chunkMetadata);
      }
      return currentPair;
    }

    public Pair<String, IChunkMetadata> getCurrentPair() {
      return currentPair;
    }

    private byte readNextChunkMetadataType() throws IOException {
      typeBuffer.clear();
      ReadWriteIOUtils.readAsPossible(input, typeBuffer);
      typeBuffer.flip();
      return ReadWriteIOUtils.readByte(typeBuffer);
    }

    private int readNextChunkMetadataSize() throws IOException {
      sizeBuffer.clear();
      ReadWriteIOUtils.readAsPossible(input, sizeBuffer);
      sizeBuffer.flip();
      return ReadWriteIOUtils.readInt(sizeBuffer);
    }
  }
}
