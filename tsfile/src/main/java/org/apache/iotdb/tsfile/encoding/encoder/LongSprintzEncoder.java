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

import org.apache.iotdb.tsfile.encoding.bitpacking.LongPacker;
import org.apache.iotdb.tsfile.encoding.fire.LongFire;
import org.apache.iotdb.tsfile.exception.encoding.TsFileEncodingException;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Vector;

public class LongSprintzEncoder extends SprintzEncoder {

  /** bit packer */
  LongPacker packer;

  /** Long Fire predictor * */
  LongFire firePred;

  /** we save all value in a list and calculate its bitwidth. */
  protected Vector<Long> values;

  public LongSprintzEncoder() {
    super();
    values = new Vector<>();
    firePred = new LongFire(3);
  }

  @Override
  protected void reset() {
    super.reset();
    values.clear();
  }

  @Override
  public int getOneItemMaxSize() {
    return 1 + (1 + Block_size) * Long.BYTES;
  }

  @Override
  public long getMaxByteSize() {
    return 1 + (1 + values.size()) * Long.BYTES;
  }

  protected Long predict(Long value, Long preVlaue) throws TsFileEncodingException {
    Long pred;
    if (PredictMethod.equals("delta")) {
      pred = delta(value, preVlaue);
    } else if (PredictMethod.equals("fire")) {
      pred = fire(value, preVlaue);
    } else {
      throw new TsFileEncodingException(
          "Config: Predict Method {} of SprintzEncoder is not supported.");
    }
    if (pred <= 0) pred = -2 * pred;
    else pred = 2 * pred - 1; // TODO:overflow
    return pred;
  }

  @Override
  protected void bitPack() throws IOException {
    long preValue = values.get(0);
    values.remove(0);
    this.bitWidth = ReadWriteForEncodingUtils.getLongMaxBitWidth(values);
    packer = new LongPacker(this.bitWidth);
    byte[] bytes = new byte[bitWidth];
    long[] tmpBuffer = new long[Block_size];
    for (int i = 0; i < Block_size; i++) tmpBuffer[i] = values.get(i);
    packer.pack8Values(tmpBuffer, 0, bytes);
    ReadWriteForEncodingUtils.writeIntLittleEndianPaddedOnBitWidth(bitWidth, byteCache, 1);
    byteCache.write(ByteBuffer.allocate(8).putLong(preValue).array());
    byteCache.write(bytes, 0, bytes.length);
  }

  protected Long delta(Long value, Long preValue) {
    return value - preValue;
  }

  protected Long fire(Long value, Long preValue) {
    long pred = firePred.predict(preValue);
    long err = value - pred;
    firePred.train(preValue, value, err);
    return err;
  }

  @Override
  protected void entropy() {
    // TODO
  }

  @Override
  public void flush(ByteArrayOutputStream out) throws IOException {
    if (byteCache.size() > 0) {
      byteCache.writeTo(out);
    }
    if (!values.isEmpty()) {
      int size = values.size();
      size |= (1 << 7);
      ReadWriteForEncodingUtils.writeIntLittleEndianPaddedOnBitWidth(size, out, 1);
      LongRleEncoder encoder = new LongRleEncoder();
      for (long val : values) {
        encoder.encode(val, out);
      }
      encoder.flush(out);
    }
    reset();
  }

  @Override
  public void encode(long value, ByteArrayOutputStream out) {
    if (!isFirstCached) {
      values.add(value);
      isFirstCached = true;
      return;
    } else {
      values.add(value);
    }
    if (values.size() == Block_size + 1) {
      try {
        long pre = values.get(0);
        firePred.reset();
        for (int i = 1; i <= Block_size; i++) {
          long tmp = values.get(i);
          values.set(i, predict(values.get(i), pre));
          pre = tmp;
        }
        bitPack();
        isFirstCached = false;
        values.clear();
        groupNum++;
        if (groupNum == groupMax) {
          flush(out);
        }
      } catch (IOException e) {
        logger.error("Error occured when encoding INT64 Type value with with Sprintz", e);
      }
    }
  }
}
