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

package org.apache.iotdb.tsfile.write.writer.tsmiterator;

import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.LocalTsFileInput;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * This class read ChunkMetadata iteratively from disk(.cmt file) and memory(list of
 * ChunkGroupMetadata), and construct them as TimeseriesMetadata. It will read ChunkMetadata in disk
 * first, and after all ChunkMetadata in disk is read, it will read ChunkMetadata in memory.
 */
public class DiskTSMIterator extends TSMIterator {

  private static final Logger LOG = LoggerFactory.getLogger(DiskTSMIterator.class);

  private LinkedList<Long> endPosForEachDevice;
  private File cmtFile;
  private LocalTsFileInput input;
  private long fileLength = 0;
  private long currentPos = 0;
  private long nextEndPosForDevice = 0;
  private String currentDevice;
  private boolean remainsInFile = true;

  protected DiskTSMIterator(
      File cmtFile,
      List<ChunkGroupMetadata> chunkGroupMetadataList,
      LinkedList<Long> endPosForEachDevice)
      throws IOException {
    super(chunkGroupMetadataList);
    this.cmtFile = cmtFile;
    this.endPosForEachDevice = endPosForEachDevice;
    this.input = new LocalTsFileInput(cmtFile.toPath());
    this.fileLength = cmtFile.length();
    this.nextEndPosForDevice = endPosForEachDevice.removeFirst();
  }

  @Override
  public boolean hasNext() {
    return remainsInFile || iterator.hasNext();
  }

  @Override
  public Pair<Path, TimeseriesMetadata> next() {
    try {
      if (remainsInFile) {
        // deserialize from file
        return getTimeSerisMetadataFromFile();
      } else {
        // get from memory iterator
        return super.next();
      }
    } catch (IOException e) {
      LOG.error("Meets IOException when reading timeseries metadata from disk", e);
      return null;
    }
  }

  private Pair<Path, TimeseriesMetadata> getTimeSerisMetadataFromFile() throws IOException {
    if (currentPos == nextEndPosForDevice) {
      // deserialize the current device name
      currentDevice = ReadWriteIOUtils.readString(input.wrapAsInputStream());
      nextEndPosForDevice =
          endPosForEachDevice.size() > 0 ? endPosForEachDevice.removeFirst() : fileLength;
    }
    // deserialize public info for measurement
    String measurementUid = ReadWriteIOUtils.readVarIntString(input.wrapAsInputStream());
    byte dataTypeInByte = ReadWriteIOUtils.readByte(input.wrapAsInputStream());
    TSDataType dataType = TSDataType.getTsDataType(dataTypeInByte);
    int chunkBufferSize = ReadWriteIOUtils.readInt(input.wrapAsInputStream());
    ByteBuffer chunkBuffer = ByteBuffer.allocate(chunkBufferSize);
    int readSize = ReadWriteIOUtils.readAsPossible(input, chunkBuffer);
    if (readSize < chunkBufferSize) {
      throw new IOException(
          String.format(
              "Expected to read %s bytes, but actually read %s bytes", chunkBufferSize, readSize));
    }
    chunkBuffer.flip();

    // deserialize chunk metadata from chunk buffer
    List<IChunkMetadata> chunkMetadataList = new ArrayList<>();
    while (chunkBuffer.hasRemaining()) {
      chunkMetadataList.add(ChunkMetadata.deserializeFrom(chunkBuffer, dataType));
    }
    updateCurrentPos();
    return new Pair<>(
        new Path(currentDevice, measurementUid, false),
        constructOneTimeseriesMetadata(measurementUid, chunkMetadataList));
  }

  private void updateCurrentPos() throws IOException {
    currentPos = input.position();
    if (currentPos >= fileLength) {
      remainsInFile = false;
      input.close();
    }
  }
}
