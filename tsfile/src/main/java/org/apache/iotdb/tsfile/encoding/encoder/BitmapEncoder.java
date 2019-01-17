/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.tsfile.encoding.encoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.tsfile.encoding.common.EndianType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encodes values using bitmap, according to the following grammar:
 *
 * <pre>
 * {@code
 * bitmap-encoding: <length> <num> <encoded-data>
 * length := length of the <encoded-data> in bytes stored as unsigned var int
 * num := number for all encoded data in <encoded-data> stored as unsigned var int
 * encoded-data := <run>*
 * run := <value> <bit-index>
 * value := value in the data after deduplication. Use varint-encode and store as unsigned var int
 * bit-index := a list of 01 sequence to record the position of the value above
 * }
 * </pre>.
 */
@Deprecated
public class BitmapEncoder extends Encoder {

  private static final Logger LOGGER = LoggerFactory.getLogger(BitmapEncoder.class);

  /**
   * Bitmap Encoder stores all current values in a list temporally.
   */
  private List<Integer> values;

  /**
   * BitmapEncoder constructor.
   *
   * @param endianType deprecated
   */
  public BitmapEncoder(EndianType endianType) {
    super(TSEncoding.BITMAP);
    this.values = new ArrayList<Integer>();
    LOGGER.debug("tsfile-encoding BitmapEncoder: init bitmap encoder");
  }

  /**
   * Each time encoder receives a value, encoder doesn't write it to OutputStream immediately.
   * Encoder stores current value in a list. When all value is received, flush() method will be
   * invoked. Encoder encodes all values and writes them to OutputStream.
   *
   * @param value value to encode
   * @param out OutputStream to write encoded stream
   * @throws IOException cannot encode value
   * @see Encoder#encode(int, java.io.ByteArrayOutputStream)
   */
  @Override
  public void encode(int value, ByteArrayOutputStream out) throws IOException {
    values.add(value);
  }

  /**
   * When all data received, encoder now encodes values in list and write them to OutputStream.
   *
   * @param out OutputStream to write encoded stream
   * @throws IOException cannot flush to OutputStream
   * @see Encoder#flush(java.io.ByteArrayOutputStream)
   */
  @Override
  public void flush(ByteArrayOutputStream out) throws IOException {
    // byteCache stores all <encoded-data> and we know its size
    ByteArrayOutputStream byteCache = new ByteArrayOutputStream();
    Set<Integer> valueType = new HashSet<Integer>(values);
    int byteNum = (values.size() + 7) / 8;
    if (byteNum == 0) {
      reset();
      return;
    }
    int len = values.size();
    // LOGGER.debug("tsfile-encoding BitmapEncoder: number of data in list is {}", len);
    for (int value : valueType) {
      byte[] buffer = new byte[byteNum];
      for (int i = 0; i < len; i++) {
        if (values.get(i) == value) {
          int index = i / 8;
          int offset = 7 - (i % 8);
          // Encoder use 1 bit in byte to indicate that value appears
          buffer[index] |= ((byte) 1 << offset);
        }
      }
      ReadWriteForEncodingUtils.writeUnsignedVarInt(value, byteCache);
      byteCache.write(buffer);
    }
    ReadWriteForEncodingUtils.writeUnsignedVarInt(byteCache.size(), out);
    ReadWriteForEncodingUtils.writeUnsignedVarInt(len, out);
    out.write(byteCache.toByteArray());
    reset();
  }

  private void reset() {
    values.clear();
  }

  @Override
  public int getOneItemMaxSize() {
    return 1;
  }

  @Override
  public long getMaxByteSize() {
    // byteCacheSize + byteDictSize + (byte array + array length) * byteDictSize
    return 4 + 4 + ((values.size() + 7) / 8 + 4) * values.size();
  }
}
