/**
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
package org.apache.iotdb.db.engine.overflow.ioV2;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.engine.overflow.metadata.OFRowGroupListMetadata;
import org.apache.iotdb.db.engine.overflow.metadata.OFSeriesListMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileOutput;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class OverflowIO extends TsFileIOWriter {

  private OverflowReadWriter overflowReadWriter;

  public OverflowIO(OverflowReadWriter overflowReadWriter) throws IOException {
    super(overflowReadWriter, new ArrayList<>());
    this.overflowReadWriter = overflowReadWriter;
    toTail();
  }

  /**
   * read one time-series chunk data and wrap these bytes into a input stream.
   *
   * @param chunkMetaData
   * @return one input stream contains the data of the time-series chunk metadata.
   * @throws IOException
   */
  @Deprecated
  public static InputStream readOneTimeSeriesChunk(ChunkMetaData chunkMetaData,
      TsFileInput fileReader)
      throws IOException {
    /*
     * long begin = chunkMetaData.getOffsetOfChunkHeader(); long size = chunkMetaData.getTotalByteSize(); byte[]
     * buff = new byte[(int) size]; fileReader.position(begin); fileReader.read(buff, 0, (int) size);
     */
    ByteArrayInputStream input = new ByteArrayInputStream(null);
    return input;
  }

  @Deprecated
  private ChunkMetaData startTimeSeries(OverflowSeriesImpl index) throws IOException {
    /*
     * LOGGER.debug(
     * "Start overflow series chunk meatadata: measurementId: {}, valueCount: {}, compressionName: {}, TSdatatype: {}."
     * , index.getMeasurementId(), index.getValueCount(), CompressionTypeName.UNCOMPRESSED, index.getDataType());
     * ChunkMetaData currentSeries; currentSeries = new ChunkMetaData(index.getMeasurementId(), TSChunkType.VALUE,
     * this.getPos(), CompressionTypeName.UNCOMPRESSED); currentSeries.setNumRows(index.getValueCount()); byte[] max
     * = index.getStatistics().getMaxBytes(); byte[] min = index.getStatistics().getMinBytes();
     * VInTimeSeriesChunkMetaData vInTimeSeriesChunkMetaData = new VInTimeSeriesChunkMetaData(index.getDataType());
     * Map<String, ByteBuffer> minMaxMap = new HashMap<>(); minMaxMap.put(AggregationConstant.MIN_VALUE,
     * ByteBuffer.wrap(min)); minMaxMap.put(AggregationConstant.MAX_VALUE, ByteBuffer.wrap(max)); TsDigest tsDigest
     * = new TsDigest(minMaxMap); vInTimeSeriesChunkMetaData.setDigest(tsDigest);
     * currentSeries.setVInTimeSeriesChunkMetaData(vInTimeSeriesChunkMetaData); return currentSeries;
     */
    return null;
  }

  @Deprecated
  private ChunkMetaData endSeries(long size, ChunkMetaData currentSeries) throws IOException {
    /*
     * currentSeries.setTotalByteSize(size); return currentSeries;
     */
    return null;
  }

  /**
   * flush one overflow time-series tree to file, and get the time-series chunk meta-data.
   *
   * @param index
   * @return the time-series chunk meta-data corresponding to the overflow time-series tree.
   * @throws IOException
   */
  @Deprecated
  public ChunkMetaData flush(OverflowSeriesImpl index) throws IOException {
    /*
     * long beginPos = this.getPos(); ChunkMetaData currentSeries = startTimeSeries(index);
     *
     * ByteArrayOutputStream byteStream = new ByteArrayOutputStream(); index.getOverflowIndex().toBytes(byteStream);
     * byteStream.writeTo(overflowReadWriter); // TODO: use buff // flush(); int size = (int) (this.getPos() -
     * beginPos); currentSeries.setTotalByteSize(size); endSeries(size, currentSeries); return currentSeries;
     */
    return null;
  }

  public void clearRowGroupMetadatas() {
    super.chunkGroupMetaDataList.clear();
  }

  @Deprecated
  public List<OFRowGroupListMetadata> flush(
      Map<String, Map<String, OverflowSeriesImpl>> overflowTrees)
      throws IOException {
    List<OFRowGroupListMetadata> ofRowGroupListMetadatas = new ArrayList<OFRowGroupListMetadata>();
    if (overflowTrees.isEmpty()) {
      return ofRowGroupListMetadatas;
    } else {
      for (String deviceId : overflowTrees.keySet()) {
        Map<String, OverflowSeriesImpl> seriesMap = overflowTrees.get(deviceId);
        OFRowGroupListMetadata rowGroupListMetadata = new OFRowGroupListMetadata(deviceId);
        for (String measurementId : seriesMap.keySet()) {
          ChunkMetaData current = flush(seriesMap.get(measurementId));
          ArrayList<ChunkMetaData> timeSeriesList = new ArrayList<>();
          timeSeriesList.add(current);
          // TODO : optimize the OFSeriesListMetadata
          OFSeriesListMetadata ofSeriesListMetadata = new OFSeriesListMetadata(measurementId,
              timeSeriesList);
          rowGroupListMetadata.addSeriesListMetaData(ofSeriesListMetadata);
        }
        ofRowGroupListMetadatas.add(rowGroupListMetadata);
      }
    }
    flush();
    return ofRowGroupListMetadatas;
  }

  public void toTail() throws IOException {
    overflowReadWriter.toTail();
  }

  public long getPos() throws IOException {
    return overflowReadWriter.getPosition();
  }

  public void close() throws IOException {
    overflowReadWriter.close();
  }

  public void flush() throws IOException {
    overflowReadWriter.flush();
  }

  public OverflowReadWriter getReader() {
    return overflowReadWriter;
  }

  public OverflowReadWriter getWriter() {
    return overflowReadWriter;
  }

  public static class OverflowReadWriter extends OutputStream implements TsFileOutput, TsFileInput {

    private static final String RW_MODE = "rw";
    private RandomAccessFile raf;

    public OverflowReadWriter(String filepath) throws FileNotFoundException {
      this.raf = new RandomAccessFile(filepath, RW_MODE);
    }

    public void toTail() throws IOException {
      raf.seek(raf.length());
    }

    @Override
    public long size() throws IOException {
      return raf.length();
    }

    @Override
    public long position() throws IOException {
      return raf.getFilePointer();
    }

    @Override
    public TsFileInput position(long newPosition) throws IOException {
      raf.seek(newPosition);
      return this;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
      throw new NotImplementedException();
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
      throw new NotImplementedException();
    }

    @Override
    public FileChannel wrapAsFileChannel() throws IOException {
      return raf.getChannel();
    }

    @Override
    public InputStream wrapAsInputStream() throws IOException {
      return Channels.newInputStream(raf.getChannel());
    }

    @Override
    public int read() throws IOException {
      return raf.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      raf.readFully(b, off, len);
      return len;
    }

    @Override
    public int readInt() throws IOException {
      return raf.readInt();
    }

    @Override
    public void write(ByteBuffer b) throws IOException {
      throw new NotImplementedException();
    }

    @Override
    public long getPosition() throws IOException {
      return raf.getFilePointer();
    }

    @Override
    public OutputStream wrapAsStream() throws IOException {
      return Channels.newOutputStream(raf.getChannel());
    }

    @Override
    public void truncate(long position) throws IOException {
      raf.getChannel().truncate(position);
    }

    @Override
    public void write(int b) throws IOException {
      raf.write(b);
    }

    @Override
    public void close() throws IOException {
      raf.close();
    }
  }
}
