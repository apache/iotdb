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

import org.apache.iotdb.tsfile.encoding.bitpacking.IntPacker;
import org.apache.iotdb.tsfile.encoding.fire.IntFire;
import org.apache.iotdb.tsfile.exception.encoding.TsFileEncodingException;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Vector;

public class IntSprintzEncoder extends SprintzEncoder {

  /** bit packer */
  IntPacker packer;

  /** Fire predictor * */
  IntFire firePred;

  /** we save all value in a list and calculate its bitwidth. */
  protected Vector<Integer> values;

  public IntSprintzEncoder() {
    super();
    values = new Vector<>();
    firePred = new IntFire(2);
  }

  @Override
  protected void reset() {
    super.reset();
    values.clear();
  }

  @Override
  public int getOneItemMaxSize() {
    return 1 + (1 + Block_size) * Integer.BYTES;
  }

  @Override
  public long getMaxByteSize() {
    return 1 + (long) (values.size() + 1) * Integer.BYTES;
  }

  protected Integer predict(Integer value, Integer preVlaue) throws TsFileEncodingException {
    Integer pred;
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
    int preValue = values.get(0);
    values.remove(0);
    this.bitWidth = ReadWriteForEncodingUtils.getIntMaxBitWidth(values);
    packer = new IntPacker(this.bitWidth);
    byte[] bytes = new byte[bitWidth];
    int[] tmpBuffer = new int[Block_size];
    for (int i = 0; i < Block_size; i++) tmpBuffer[i] = values.get(i);
    packer.pack8Values(tmpBuffer, 0, bytes);
    ReadWriteForEncodingUtils.writeIntLittleEndianPaddedOnBitWidth(bitWidth, byteCache, 1);
    ReadWriteForEncodingUtils.writeUnsignedVarInt(preValue, byteCache);
    byteCache.write(bytes, 0, bytes.length);
  }

  protected Integer delta(Integer value, Integer preValue) {
    return value - preValue;
  }

  protected Integer fire(Integer value, Integer preValue) {
    int pred = firePred.predict(preValue);
    int err = value - pred;
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
      IntRleEncoder encoder = new IntRleEncoder();
      for (int val : values) {
        encoder.encode(val, out);
      }
      encoder.flush(out);
    }
    reset();
  }

  @Override
  public void encode(int value, ByteArrayOutputStream out) {
    if (!isFirstCached) {
      values.add(value);
      isFirstCached = true;
      return;
    } else {
      values.add(value);
    }
    if (values.size() == Block_size + 1) {
      try {
        int pre = values.get(0);
        firePred.reset();
        for (int i = 1; i <= Block_size; i++) {
          int tmp = values.get(i);
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
        logger.error("Error occured when encoding INT32 Type value with with Sprintz", e);
      }
    }

  }
}
