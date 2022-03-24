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

package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.bitpacking.IntPacker;
import org.apache.iotdb.tsfile.encoding.fire.IntFire;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class FloatSprintzDecoder extends SprintzDecoder {

  IntPacker packer;
  IntFire firePred;
  private float preValue;
  private final float[] currentBuffer;
  private final int[] convertBuffer;
  private float currentValue;
  private final String predictScheme =
      TSFileDescriptor.getInstance().getConfig().getSprintzPredictScheme();

  public FloatSprintzDecoder() {
    super();
    currentBuffer = new float[Block_size + 1];
    convertBuffer = new int[Block_size];
    firePred = new IntFire(2);
    reset();
  }

  @Override
  public boolean hasNext(ByteBuffer buffer) throws IOException {
    int minLenth = Float.BYTES + 1;
    return (isBlockReaded && currentCount < Block_size) || buffer.remaining() >= minLenth;
  }

  @Override
  public void reset() {
    super.reset();
    currentValue = 0;
    preValue = 0;
    currentCount = 0;
    Arrays.fill(currentBuffer, 0);
    Arrays.fill(convertBuffer, 0);
  }

  @Override
  protected void decodeBlock(ByteBuffer in) throws IOException {
    bitWidth = ReadWriteForEncodingUtils.readIntLittleEndianPaddedOnBitWidth(in, 1);
    if ((bitWidth & (1 << 7)) != 0) {
      decodeSize = bitWidth & ~(1 << 7);
      SinglePrecisionDecoderV2 decoder = new SinglePrecisionDecoderV2();
      for (int i = 0; i < decodeSize; i++) {
        currentBuffer[i] = decoder.readFloat(in);
      }
    } else {
      decodeSize = Block_size + 1;
      preValue = in.getFloat();
      currentBuffer[0] = preValue;
      int[] tmpBuffer = new int[8];
      packer = new IntPacker(bitWidth);
      byte[] packcle = new byte[bitWidth];
      for (int i = 0; i < bitWidth; i++) {
        packcle[i] = in.get();
      }
      packer.unpack8Values(packcle, 0, tmpBuffer);
      for (int i = 0; i < 8; i++) convertBuffer[i] = tmpBuffer[i];
      recalculate();
    }
    isBlockReaded = true;
  }

  @Override
  protected void recalculate() {
    for (int i = 0; i < Block_size; i++) {
      if (convertBuffer[i] % 2 == 0) convertBuffer[i] = -convertBuffer[i] / 2;
      else convertBuffer[i] = (convertBuffer[i] + 1) / 2;
    }
    if (predictScheme.equals("delta")) {
      convertBuffer[0] = convertBuffer[0] + Float.floatToIntBits(preValue);
      currentBuffer[1] = Float.intBitsToFloat(convertBuffer[0]);
      for (int i = 1; i < Block_size; i++) {
        convertBuffer[i] += convertBuffer[i - 1];
        currentBuffer[i + 1] = Float.intBitsToFloat(convertBuffer[i]);
      }
    } else if (predictScheme.equals("fire")) {
      firePred.reset();
      int p = firePred.predict(Float.floatToIntBits(preValue));
      int e = convertBuffer[0];
      convertBuffer[0] += p;
      currentBuffer[1] = Float.intBitsToFloat(convertBuffer[0]);
      firePred.train(Float.floatToIntBits(preValue), convertBuffer[0], e);
      for (int i = 1; i < Block_size; i++) {
        int pred = firePred.predict(convertBuffer[i - 1]);
        int err = convertBuffer[i];
        convertBuffer[i] += pred;
        currentBuffer[i + 1] = Float.intBitsToFloat(convertBuffer[i]);
        firePred.train(convertBuffer[i - 1], convertBuffer[i], err);
      }
    } else {
      throw new UnsupportedOperationException("Sprintz predictive method {} is not supported.");
    }
  }

  @Override
  public float readFloat(ByteBuffer buffer) {
    logger.error("Decode SPRINTZ start");
    if (!isBlockReaded) {
      try {
        decodeBlock(buffer);
      } catch (IOException e) {
        logger.error("Error occured when readInt with Sprintz Decoder.", e);
      }
    }
    currentValue = currentBuffer[currentCount++];
    if (currentCount == decodeSize) {
      isBlockReaded = false;
      currentCount = 0;
    }
    logger.error("Decode SPRINTZ stop");
    return currentValue;
  }
}
