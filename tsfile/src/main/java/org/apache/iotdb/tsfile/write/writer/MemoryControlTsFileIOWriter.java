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

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexEntry;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexNode;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.LocalTsFileInput;
import org.apache.iotdb.tsfile.utils.BloomFilter;
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
import java.util.TreeMap;

import static org.apache.iotdb.tsfile.file.metadata.MetadataIndexConstructor.addCurrentIndexNodeToQueue;
import static org.apache.iotdb.tsfile.file.metadata.MetadataIndexConstructor.checkAndBuildLevelIndex;
import static org.apache.iotdb.tsfile.file.metadata.MetadataIndexConstructor.generateRootNode;

/**
 * This writer control the total size of chunk metadata to avoid OOM when writing massive
 * timeseries. <b>This writer can only be used in the scenarios where the chunk is written in
 * order.</b> The order means lexicographical order and time order. The lexicographical order
 * requires that, if the writer is going to write a series <i>S</i>, all data of the all series
 * smaller than <i>S</i> in lexicographical order has been written to the writer. The time order
 * requires that, for a single series <i>S</i>, if the writer is going to write a chunk <i>C</i> of
 * it, all chunks of <i>S</i> whose start time is smaller than <i>C</i> should have been written to
 * the writer. If you do not comply with the above requirements, metadata index tree may be
 * generated incorrectly. As a result, the file cannot be queried correctly.
 */
public class MemoryControlTsFileIOWriter extends TsFileIOWriter {
  private static final Logger LOG = LoggerFactory.getLogger(MemoryControlTsFileIOWriter.class);
  protected long maxMetadataSize;
  protected long currentChunkMetadataSize = 0L;
  protected File chunkMetadataTempFile;
  protected LocalTsFileOutput tempOutput;
  protected volatile boolean hasChunkMetadataInDisk = false;
  protected String currentSeries = null;
  // record the total num of path in order to make bloom filter
  protected int pathCount = 0;
  Path lastSerializePath = null;

  public static final String CHUNK_METADATA_TEMP_FILE_PREFIX = ".cmt";
  private static final byte NORMAL_TYPE = 2;

  public MemoryControlTsFileIOWriter(File file, long maxMetadataSize) throws IOException {
    super(file);
    this.maxMetadataSize = maxMetadataSize;
    this.chunkMetadataTempFile = new File(file.getAbsoluteFile() + CHUNK_METADATA_TEMP_FILE_PREFIX);
  }

  @Override
  public void endCurrentChunk() {
    currentChunkMetadataSize += currentChunkMetadata.calculateRamSize();
    super.endCurrentChunk();
  }

  /**
   * Check if the size of chunk metadata in memory is greater than the given threshold. If so, the
   * chunk metadata will be written to a temp files. <b>Notice! If you are writing a aligned device,
   * you should make sure all data of current writing device has been written before this method is
   * called.</b> For not aligned series, there is no such limitation.
   *
   * @throws IOException
   */
  public void checkMetadataSizeAndMayFlush() throws IOException {
    // This function should be called after all data of an aligned device has been written
    if (currentChunkMetadataSize > maxMetadataSize) {
      try {
        sortAndFlushChunkMetadata();
      } catch (IOException e) {
        LOG.error("Meets exception when flushing metadata to temp file for {}", file, e);
        throw e;
      }
    }
  }

  /**
   * Sort the chunk metadata by the lexicographical order and the start time of the chunk, then
   * flush them to a temp file.
   *
   * @throws IOException
   */
  protected void sortAndFlushChunkMetadata() throws IOException {
    // group by series
    Map<Path, List<IChunkMetadata>> chunkMetadataListMap = groupChunkMetadataListBySeries();
    if (tempOutput == null) {
      tempOutput = new LocalTsFileOutput(new FileOutputStream(chunkMetadataTempFile));
    }
    hasChunkMetadataInDisk = true;
    // the file structure in temp file will be
    // chunkSize | chunkBuffer
    for (Map.Entry<Path, List<IChunkMetadata>> entry : chunkMetadataListMap.entrySet()) {
      Path seriesPath = entry.getKey();
      if (!seriesPath.equals(lastSerializePath)) {
        // record the count of path to construct bloom filter later
        pathCount++;
      }
      List<IChunkMetadata> iChunkMetadataList = entry.getValue();
      writeChunkMetadata(iChunkMetadataList, seriesPath, tempOutput);
      lastSerializePath = seriesPath;
    }
    // clear the cache metadata to release the memory
    chunkGroupMetadataList.clear();
    if (chunkMetadataList != null) {
      chunkMetadataList.clear();
    }
  }

  private void writeChunkMetadata(
      List<IChunkMetadata> iChunkMetadataList, Path seriesPath, LocalTsFileOutput output)
      throws IOException {
    for (IChunkMetadata chunkMetadata : iChunkMetadataList) {
      PublicBAOS buffer = new PublicBAOS();
      int size = chunkMetadata.serializeWithFullInfo(buffer, seriesPath.getFullPath());
      ReadWriteIOUtils.write(size, output);
      buffer.writeTo(output);
    }
  }

  @Override
  public void endFile() throws IOException {
    if (!hasChunkMetadataInDisk) {
      // all the chunk metadata is stored in memory
      // sort the chunk metadata, construct the index tree
      // and just close the file
      super.endFile();
      return;
    }

    // there is some chunk metadata already been written to the disk
    // first we should flush the remaining chunk metadata in memory to disk
    // then read the persisted chunk metadata from disk
    sortAndFlushChunkMetadata();
    tempOutput.close();

    // read in the chunk metadata, and construct the index tree
    readChunkMetadataAndConstructIndexTree();

    // write magic string
    out.write(MAGIC_STRING_BYTES);

    // close file
    out.close();
    canWrite = false;
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
    Map<String, MetadataIndexNode> deviceMetadataIndexMap = new TreeMap<>();
    Queue<MetadataIndexNode> measurementMetadataIndexQueue = new ArrayDeque<>();
    String currentDevice = null;
    String prevDevice = null;
    MetadataIndexNode currentIndexNode =
        new MetadataIndexNode(MetadataIndexNodeType.LEAF_MEASUREMENT);
    TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
    int seriesIdxForCurrDevice = 0;
    BloomFilter filter =
        BloomFilter.getEmptyBloomFilter(
            TSFileDescriptor.getInstance().getConfig().getBloomFilterErrorRate(), pathCount);

    int indexCount = 0;
    while (iterator.hasNextChunkMetadata()) {
      // read in all chunk metadata of one series
      // construct the timeseries metadata for this series
      TimeseriesMetadata timeseriesMetadata = readTimeseriesMetadata(iterator);

      indexCount++;
      // build bloom filter
      filter.add(currentSeries);
      // construct the index tree node for the series
      Path currentPath = null;
      if (timeseriesMetadata.getTSDataType() == TSDataType.VECTOR) {
        // this series is the time column of the aligned device
        // the full series path will be like "root.sg.d."
        // we remove the last . in the series id here
        currentDevice = currentSeries.substring(0, currentSeries.length() - 1);
      } else {
        currentPath = new Path(currentSeries, true);
        currentDevice = currentPath.getDevice();
      }
      if (!currentDevice.equals(prevDevice)) {
        if (prevDevice != null) {
          addCurrentIndexNodeToQueue(currentIndexNode, measurementMetadataIndexQueue, out);
          deviceMetadataIndexMap.put(
              prevDevice,
              generateRootNode(
                  measurementMetadataIndexQueue, out, MetadataIndexNodeType.INTERNAL_MEASUREMENT));
          currentIndexNode = new MetadataIndexNode(MetadataIndexNodeType.LEAF_MEASUREMENT);
        }
        measurementMetadataIndexQueue = new ArrayDeque<>();
        seriesIdxForCurrDevice = 0;
      }

      if (seriesIdxForCurrDevice % config.getMaxDegreeOfIndexNode() == 0) {
        if (currentIndexNode.isFull()) {
          addCurrentIndexNodeToQueue(currentIndexNode, measurementMetadataIndexQueue, out);
          currentIndexNode = new MetadataIndexNode(MetadataIndexNodeType.LEAF_MEASUREMENT);
        }
        if (timeseriesMetadata.getTSDataType() != TSDataType.VECTOR) {
          currentIndexNode.addEntry(
              new MetadataIndexEntry(currentPath.getMeasurement(), out.getPosition()));
        } else {
          currentIndexNode.addEntry(new MetadataIndexEntry("", out.getPosition()));
        }
      }

      prevDevice = currentDevice;
      seriesIdxForCurrDevice++;
      // serialize the timeseries metadata to file
      timeseriesMetadata.serializeTo(out.wrapAsStream());
    }

    addCurrentIndexNodeToQueue(currentIndexNode, measurementMetadataIndexQueue, out);
    deviceMetadataIndexMap.put(
        prevDevice,
        generateRootNode(
            measurementMetadataIndexQueue, out, MetadataIndexNodeType.INTERNAL_MEASUREMENT));

    if (indexCount != pathCount) {
      throw new IOException(
          String.format(
              "Expected path count is %d, index path count is %d", pathCount, indexCount));
    }

    MetadataIndexNode metadataIndex = checkAndBuildLevelIndex(deviceMetadataIndexMap, out);

    TsFileMetadata tsFileMetadata = new TsFileMetadata();
    tsFileMetadata.setMetadataIndex(metadataIndex);
    tsFileMetadata.setMetaOffset(metaOffset);

    int size = tsFileMetadata.serializeTo(out.wrapAsStream());
    size += tsFileMetadata.serializeBloomFilter(out.wrapAsStream(), filter);

    // write TsFileMetaData size
    ReadWriteIOUtils.write(size, out.wrapAsStream());
  }

  /**
   * Read in all the chunk metadata for a series, and construct a TimeseriesMetadata for it
   *
   * @param iterator
   * @return
   * @throws IOException
   */
  private TimeseriesMetadata readTimeseriesMetadata(ChunkMetadataReadIterator iterator)
      throws IOException {
    List<IChunkMetadata> iChunkMetadataList = new ArrayList<>();
    currentSeries = iterator.getAllChunkMetadataForNextSeries(iChunkMetadataList);
    TimeseriesMetadata timeseriesMetadata =
        super.constructOneTimeseriesMetadata(new Path(currentSeries), iChunkMetadataList, false);
    if (timeseriesMetadata.getTSDataType() == TSDataType.VECTOR) {
      // set empty measurement id for time column
      timeseriesMetadata.setMeasurementId("");
    } else {
      timeseriesMetadata.setMeasurementId(new Path(currentSeries, true).getMeasurement());
    }
    return timeseriesMetadata;
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

    /**
     * Read in next chunk, return the series full path and the chunk metadata.
     *
     * @return
     * @throws IOException
     */
    protected Pair<String, IChunkMetadata> getNextSeriesNameAndChunkMetadata() throws IOException {
      if (input.position() >= endPosition) {
        currentPair = null;
        return null;
      }
      int size = readNextChunkMetadataSize();
      ByteBuffer chunkBuffer = ByteBuffer.allocate(size);
      ReadWriteIOUtils.readAsPossible(input, chunkBuffer);
      chunkBuffer.flip();
      ChunkMetadata chunkMetadata = new ChunkMetadata();
      String seriesPath = ChunkMetadata.deserializeWithFullInfo(chunkBuffer, chunkMetadata);
      currentPair = new Pair<>(seriesPath, chunkMetadata);
      return currentPair;
    }

    public String getAllChunkMetadataForNextSeries(List<IChunkMetadata> iChunkMetadataList)
        throws IOException {
      // TODO: read all the chunk metadata of a single series once instead of reading it iteratively
      if (currentPair == null) {
        if (!hasNextChunkMetadata()) {
          return null;
        } else {
          getNextSeriesNameAndChunkMetadata();
        }
      }
      String currentSeries = currentPair.left;
      iChunkMetadataList.add(currentPair.right);
      while (hasNextChunkMetadata()) {
        getNextSeriesNameAndChunkMetadata();
        if (currentPair != null && currentPair.left.equals(currentSeries)) {
          iChunkMetadataList.add(currentPair.right);
        } else {
          break;
        }
      }
      return currentSeries;
    }

    public Pair<String, IChunkMetadata> getCurrentPair() {
      return currentPair;
    }

    private int readNextChunkMetadataSize() throws IOException {
      sizeBuffer.clear();
      ReadWriteIOUtils.readAsPossible(input, sizeBuffer);
      sizeBuffer.flip();
      return ReadWriteIOUtils.readInt(sizeBuffer);
    }
  }
}
