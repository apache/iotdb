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

package org.apache.iotdb.session.rpccompress;

import org.apache.iotdb.session.util.SessionUtils;

import org.apache.tsfile.compress.ICompressor;
import org.apache.tsfile.encoding.encoder.Encoder;
import org.apache.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class TabletEncoder {
  private final CompressionType compressionType;
  private final List<TSEncoding> encodingList;

  /**
   * @param compressionType compression type
   * @param encodingList the first element is for the time column
   */
  public TabletEncoder(CompressionType compressionType, List<TSEncoding> encodingList) {
    this.compressionType = compressionType;
    this.encodingList = encodingList;
  }

  public ByteBuffer encodeTime(Tablet tablet) {
    if (encodingList.get(0) == TSEncoding.PLAIN) {
      // the same bytes as PlainEncoder (big-endian longs), written in one go
      return compressBuffer(SessionUtils.getTimeBuffer(tablet));
    }
    Encoder encoder =
        TSEncodingBuilder.getEncodingBuilder(encodingList.get(0)).getEncoder(TSDataType.INT64);
    PublicBAOS baos = new PublicBAOS();
    for (int i = 0; i < tablet.getRowSize(); i++) {
      encoder.encode(tablet.getTimestamp(i), baos);
    }
    try {
      encoder.flush(baos);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }

    ByteBuffer buffer = ByteBuffer.wrap(baos.getBuf(), 0, baos.size());
    return compressBuffer(buffer);
  }

  public ByteBuffer encodeValues(Tablet tablet) {
    List<IMeasurementSchema> schemas = tablet.getSchemas();
    PublicBAOS baos = newValueStream(tablet, schemas);
    for (int j = 0, schemasSize = schemas.size(); j < schemasSize; j++) {
      TSDataType dataType = schemas.get(j).getType();
      TSEncoding encoding = encodingList.get(j + 1);
      if (encoding == TSEncoding.PLAIN) {
        // not through PlainEncoder, which writes INT32 as a var int
        SessionUtils.encodePlainValue(dataType, tablet, j, baos);
      } else {
        Encoder encoder = TSEncodingBuilder.getEncodingBuilder(encoding).getEncoder(dataType);
        SessionUtils.encodeValue(dataType, tablet, j, encoder, baos);
      }
    }

    encodeBitMap(tablet, baos);

    ByteBuffer buffer = ByteBuffer.wrap(baos.getBuf(), 0, baos.size());
    return compressBuffer(buffer);
  }

  /**
   * Pre-sizes the stream for the PLAIN columns, whose size is known up front, and the bit maps
   * after them, so that the stream does not have to double for the last few bytes. Other encodings
   * and binary values let the stream grow, as does a size that does not fit in an array.
   */
  private PublicBAOS newValueStream(Tablet tablet, List<IMeasurementSchema> schemas) {
    int rows = tablet.getRowSize();
    long size = 0;
    for (int j = 0, schemasSize = schemas.size(); j < schemasSize; j++) {
      if (encodingList.get(j + 1) == TSEncoding.PLAIN) {
        size += (long) rows * SessionUtils.getPlainRowSize(schemas.get(j).getType());
      }
    }
    if (size == 0) {
      return new PublicBAOS();
    }
    BitMap[] bitMaps = tablet.getBitMaps();
    if (bitMaps != null) {
      // at most a marker byte and the truncated bit map per column
      size += bitMaps.length * (1L + rows / Byte.SIZE + 1);
    }
    // stay clear of the VM's array size limit
    return size <= Integer.MAX_VALUE - 8 ? new PublicBAOS((int) size) : new PublicBAOS();
  }

  private void encodeBitMap(Tablet tablet, ByteArrayOutputStream baos) {
    BitMap[] bitMaps = tablet.getBitMaps();
    if (bitMaps != null) {
      try (DataOutputStream dataOutputStream = new DataOutputStream(baos)) {
        for (BitMap bitMap : bitMaps) {
          boolean columnHasNull = bitMap != null && !bitMap.isAllUnmarked(tablet.getRowSize());
          dataOutputStream.writeByte(BytesUtils.boolToByte(columnHasNull));
          if (columnHasNull) {
            dataOutputStream.write(bitMap.getTruncatedByteArray(tablet.getRowSize()));
          }
        }
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  private ByteBuffer compressBuffer(ByteBuffer buffer) {
    if (compressionType != CompressionType.UNCOMPRESSED) {
      ICompressor compressor = ICompressor.getCompressor(compressionType);
      int uncompressedSize = buffer.remaining();
      byte[] compressed = new byte[compressor.getMaxBytesForCompression(uncompressedSize) + 4];
      try {
        int compressedLength =
            compressor.compress(
                buffer.array(),
                buffer.arrayOffset() + buffer.position(),
                uncompressedSize,
                compressed);
        buffer = ByteBuffer.wrap(compressed, 0, compressedLength + 4);
        buffer.position(compressedLength);
        buffer.putInt(uncompressedSize);
        buffer.rewind();
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }
    return buffer;
  }
}
