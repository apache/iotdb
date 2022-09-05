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

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class MemoryControlTsFileIOWriter extends TsFileIOWriter {
  private static final Logger LOG = LoggerFactory.getLogger(MemoryControlTsFileIOWriter.class);
  protected long maxMetadataSize;
  protected long currentChunkMetadataSize = 0L;
  protected File chunkMetadataTempFile;
  protected LocalTsFileOutput tempOutput;
  protected final boolean needSort;
  protected Queue<Long> sortedSegmentPosition = new ArrayDeque<>();

  public static final String CHUNK_METADATA_TEMP_FILE_PREFIX = ".cmt";
  private static final String SORTING_TEMP_FILE = ".scmt";
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
      writeAlignedChunkMetadata(alignedChunkMetadata, seriesPath, output);
    } else {
      writeNormalChunkMetadata(iChunkMetadataList, seriesPath, output);
    }
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
    if (this.sortedSegmentPosition.size() > 0) {
      // there is some chunk metadata already been written to the disk
      sortAndFlushChunkMetadata();
      tempOutput.close();
    } else {
      // sort the chunk metadata in memory, and just close the file
      tempOutput.close();
      super.endFile();
      return;
    }

    if (needSort) {
      externalSort();
    }

    //		super.endFile();
  }

  protected void externalSort() throws IOException {
    ChunkMetadataComparator comparator = new ChunkMetadataComparator();
    int totalSegmentCount = this.sortedSegmentPosition.size();
    File currentInFile = this.chunkMetadataTempFile;
    File currentOutFile = new File(this.file.getAbsolutePath() + SORTING_TEMP_FILE);
    LocalTsFileInput inputForWindow1 = null;
    LocalTsFileInput inputForWindow2 = null;
    LocalTsFileOutput output = null;
    while (totalSegmentCount > 1) {
      try {
        inputForWindow1 = new LocalTsFileInput(currentInFile.toPath());
        inputForWindow2 = new LocalTsFileInput(currentInFile.toPath());
        output = new LocalTsFileOutput(new FileOutputStream(currentOutFile));
        totalSegmentCount = 0;
        Queue<Long> newSortedSegmentPosition = new ArrayDeque<>();
        while (sortedSegmentPosition.size() > 0) {
          long startPositionForWindow1 = sortedSegmentPosition.poll();
          if (sortedSegmentPosition.size() == 0) {
            // Just leave it alone, and record the position
            newSortedSegmentPosition.add(startPositionForWindow1);
            continue;
          }
          long startPositionForWindow2 = sortedSegmentPosition.poll();
          ChunkMetadataExternalSortWindow firstWindow =
              new ChunkMetadataExternalSortWindow(
                  startPositionForWindow1, startPositionForWindow2, inputForWindow1);
          ChunkMetadataExternalSortWindow secondWindow =
              new ChunkMetadataExternalSortWindow(
                  startPositionForWindow2,
                  sortedSegmentPosition.size() > 0
                      ? sortedSegmentPosition.element()
                      : this.chunkMetadataTempFile.length(),
                  inputForWindow2);
          firstWindow.getNextSeriesNameAndChunkMetadata();
          secondWindow.getNextSeriesNameAndChunkMetadata();
          newSortedSegmentPosition.add(output.getPosition());
          while (firstWindow.hasNextChunkMetadata() && secondWindow.hasNextChunkMetadata()) {
            Pair<String, IChunkMetadata> pairOfFirstWindow =
                firstWindow.getCurrentSeriesNameAndChunkMetadata();
            Pair<String, IChunkMetadata> pairOfSecondWindow =
                secondWindow.getCurrentSeriesNameAndChunkMetadata();
            Pair<String, IChunkMetadata> pairToWritten = null;
            if (comparator.compare(pairOfFirstWindow, pairOfSecondWindow) < 0) {
              pairToWritten = pairOfFirstWindow;
              if (firstWindow.hasNextChunkMetadata()) {
                firstWindow.getNextSeriesNameAndChunkMetadata();
              }
            } else {
              pairToWritten = pairOfSecondWindow;
              if (secondWindow.hasNextChunkMetadata()) {
                secondWindow.getNextSeriesNameAndChunkMetadata();
              }
            }
            // serialize the chunk to the output
            if (pairToWritten.right instanceof AlignedChunkMetadata) {
              writeAlignedChunkMetadata(
                  Collections.singletonList(pairToWritten.right),
                  new Path(pairToWritten.left),
                  output);
            } else {
              writeNormalChunkMetadata(
                  Collections.singletonList(pairToWritten.right),
                  new Path(pairToWritten.left),
                  output);
            }
          }
        }

        output.close();
        inputForWindow1.close();
        inputForWindow2.close();
        FileUtils.delete(currentInFile);
        currentOutFile.renameTo(currentInFile);
        File tempFile = currentOutFile;
        currentOutFile = currentInFile;
        currentInFile = tempFile;
      } finally {
        if (inputForWindow1 != null) {
          inputForWindow1.close();
        }
        if (inputForWindow2 != null) {
          inputForWindow2.close();
        }
        if (output != null) {
          output.close();
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    if (tempOutput != null) {
      this.tempOutput.close();
    }
  }

  protected static class ChunkMetadataComparator
      implements Comparator<Pair<String, IChunkMetadata>> {

    @Override
    public int compare(Pair<String, IChunkMetadata> o1, Pair<String, IChunkMetadata> o2) {
      String seriesNameOfO1 = o1.left;
      String seriesNameOfO2 = o2.left;
      int lexicographicalOrder = seriesNameOfO1.compareTo(seriesNameOfO2);
      if (lexicographicalOrder != 0) {
        return lexicographicalOrder;
      } else {
        return Long.compare(o1.right.getStartTime(), o2.right.getStartTime());
      }
    }
  }

  protected class ChunkMetadataExternalSortWindow {

    final LocalTsFileInput input;
    final long startPosition;
    final long endPosition;
    final ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
    final ByteBuffer typeBuffer = ByteBuffer.allocate(1);
    Pair<String, IChunkMetadata> currentPair = null;

    ChunkMetadataExternalSortWindow(long startPosition, long endPosition, LocalTsFileInput input)
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

    public Pair<String, IChunkMetadata> getCurrentSeriesNameAndChunkMetadata() {
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
