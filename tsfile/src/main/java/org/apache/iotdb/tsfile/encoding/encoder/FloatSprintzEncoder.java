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
import java.nio.ByteBuffer;
import java.util.Vector;

public class FloatSprintzEncoder extends SprintzEncoder {

  /** bit packer */
  IntPacker packer;

  /** Int Fire Predictor * */
  IntFire firePred;

  /** we save all value in a list and calculate its bitwidth. */
  protected Vector<Float> values;

  /** convert to integer Buffer * */
  int[] convertBuffer;

  public FloatSprintzEncoder() {
    super();
    values = new Vector<Float>();
    firePred = new IntFire(2);
    convertBuffer = new int[Block_size];
  }

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

  protected int predict(Float value, Float preVlaue) throws TsFileEncodingException {
    int pred;
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
    float preValue = values.get(0);
    values.remove(0);
    this.bitWidth = ReadWriteForEncodingUtils.getIntMaxBitWidth(convertBuffer, Block_size);
    packer = new IntPacker(this.bitWidth);
    byte[] bytes = new byte[bitWidth];
    packer.pack8Values(convertBuffer, 0, bytes);
    ReadWriteForEncodingUtils.writeIntLittleEndianPaddedOnBitWidth(bitWidth, byteCache, 1);
    byteCache.write(ByteBuffer.allocate(Float.BYTES).putFloat(preValue).array());
    byteCache.write(bytes, 0, bytes.length);
  }

  protected int delta(Float value, Float preValue) {
    return Float.floatToIntBits(value) - Float.floatToIntBits(preValue);
  }

  protected int fire(Float value, Float preValue) {
    int prev = Float.floatToIntBits(preValue);
    int val = Float.floatToIntBits(value);
    int pred = firePred.predict(prev);
    int err = val - pred;
    firePred.train(prev, val, err);
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
      SinglePrecisionEncoderV2 encoder = new SinglePrecisionEncoderV2();
      for (float val : values) {
        encoder.encode(val, out);
      }
      encoder.flush(out);
    }
    reset();
  }

  @Override
  public void encode(float value, ByteArrayOutputStream out) {
    if (!isFirstCached) {
      values.add(value);
      isFirstCached = true;
      return;
    } else {
      values.add(value);
    }
    if (values.size() == Block_size + 1) {
      try {
        firePred.reset();
        for (int i = 1; i <= Block_size; i++) {
          convertBuffer[i - 1] = predict(values.get(i), values.get(i - 1));
        }
        bitPack();
        isFirstCached = false;
        values.clear();
        groupNum++;
        if (groupNum == groupMax) {
          flush(out);
        }
      } catch (IOException e) {
        logger.error("Error occured when encoding Float Type value with with Sprintz", e);
      }
    }
  }
}
