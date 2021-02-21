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

package org.apache.iotdb.tsfile.encoding.encoder;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.encoding.bitpacking.IntPacker;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;

/** Encoder for int value using rle or bit-packing. */
public class IntRleEncoder extends RleEncoder<Integer> {

  /** Packer for packing int values. */
  private IntPacker packer;

  public IntRleEncoder() {
    super();
    bufferedValues = new Integer[TSFileConfig.RLE_MIN_REPEATED_NUM];
    preValue = 0;
    values = new ArrayList<>();
  }

  @Override
  public void encode(int value, ByteArrayOutputStream out) {
    values.add(value);
  }

  @Override
  public void encode(boolean value, ByteArrayOutputStream out) {
    if (value) {
      this.encode(1, out);
    } else {
      this.encode(0, out);
    }
  }

  /**
   * write all values buffered in the cache to an OutputStream.
   *
   * @param out - byteArrayOutputStream
   * @throws IOException cannot flush to OutputStream
   */
  @Override
  public void flush(ByteArrayOutputStream out) throws IOException {
    // we get bit width after receiving all data
    this.bitWidth = ReadWriteForEncodingUtils.getIntMaxBitWidth(values);
    packer = new IntPacker(bitWidth);
    for (Integer value : values) {
      encodeValue(value);
    }
    super.flush(out);
  }

  @Override
  protected void reset() {
    super.reset();
    preValue = 0;
  }

  /** write bytes to an outputStream using rle format: [header][value]. */
  @Override
  protected void writeRleRun() throws IOException {
    endPreviousBitPackedRun(TSFileConfig.RLE_MIN_REPEATED_NUM);
    ReadWriteForEncodingUtils.writeUnsignedVarInt(repeatCount << 1, byteCache);
    ReadWriteForEncodingUtils.writeIntLittleEndianPaddedOnBitWidth(preValue, byteCache, bitWidth);
    repeatCount = 0;
    numBufferedValues = 0;
  }

  @Override
  protected void clearBuffer() {

    for (int i = numBufferedValues; i < TSFileConfig.RLE_MIN_REPEATED_NUM; i++) {
      bufferedValues[i] = 0;
    }
  }

  @Override
  protected void convertBuffer() {
    byte[] bytes = new byte[bitWidth];

    int[] tmpBuffer = new int[TSFileConfig.RLE_MIN_REPEATED_NUM];
    for (int i = 0; i < TSFileConfig.RLE_MIN_REPEATED_NUM; i++) {
      tmpBuffer[i] = (int) bufferedValues[i];
    }
    packer.pack8Values(tmpBuffer, 0, bytes);
    // we'll not write bit-packing group to OutputStream immediately
    // we buffer them in list
    bytesBuffer.add(bytes);
  }

  @Override
  public int getOneItemMaxSize() {
    // The meaning of 45 is:
    // 4 + 4 + max(4+4,1 + 4 + 4 * 8)
    // length + bitwidth + max(rle-header + num, bit-header + lastNum + 8packer)
    return 45;
  }

  @Override
  public long getMaxByteSize() {
    if (values == null) {
      return 0;
    }
    // try to caculate max value
    int groupNum = (values.size() / 8 + 1) / 63 + 1;
    return (long) 8 + groupNum * 5 + values.size() * 4;
  }
}
