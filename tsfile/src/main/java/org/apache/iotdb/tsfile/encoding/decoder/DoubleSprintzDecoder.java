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
import org.apache.iotdb.tsfile.encoding.bitpacking.LongPacker;
import org.apache.iotdb.tsfile.encoding.fire.LongFire;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class DoubleSprintzDecoder extends SprintzDecoder {
  LongPacker packer;
  LongFire firePred;
  private double preValue;
  private final double[] currentBuffer;
  private final long[] convertBuffer;
  private double currentValue;
  private final String predictScheme =
      TSFileDescriptor.getInstance().getConfig().getSprintzPredictScheme();

  public DoubleSprintzDecoder() {
    super();
    currentBuffer = new double[Block_size + 1];
    convertBuffer = new long[Block_size];
    firePred = new LongFire(3);
    reset();
  }

  @Override
  public boolean hasNext(ByteBuffer buffer) throws IOException {
    // int minLenth = Long.BYTES + 1;
    return (isBlockReaded && currentCount < Block_size) || buffer.remaining() > 0;
  }

  @Override
  public void reset() {
    super.reset();
    preValue = 0;
    currentValue = 0;
    currentCount = 0;
    Arrays.fill(currentBuffer, 0);
    Arrays.fill(convertBuffer, 0);
  }

  @Override
  protected void decodeBlock(ByteBuffer in) throws IOException {
    bitWidth = ReadWriteForEncodingUtils.readIntLittleEndianPaddedOnBitWidth(in, 1);
    if ((bitWidth & (1 << 7)) != 0) {
      decodeSize = bitWidth & ~(1 << 7);
      DoublePrecisionDecoderV2 decoder = new DoublePrecisionDecoderV2();
      for (int i = 0; i < decodeSize; i++) {
        currentBuffer[i] = decoder.readDouble(in);
      }
    } else {
      decodeSize = Block_size + 1;
      preValue = in.getDouble();
      currentBuffer[0] = preValue;
      long[] tmpBuffer = new long[8];
      packer = new LongPacker(bitWidth);
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
      convertBuffer[0] = convertBuffer[0] + Double.doubleToLongBits(preValue);
      currentBuffer[1] = Double.longBitsToDouble(convertBuffer[0]);
      for (int i = 1; i < Block_size; i++) {
        convertBuffer[i] += convertBuffer[i - 1];
        currentBuffer[i + 1] = Double.longBitsToDouble(convertBuffer[i]);
      }
    } else if (predictScheme.equals("fire")) {
      firePred.reset();
      long p = firePred.predict(Double.doubleToLongBits(preValue));
      long e = convertBuffer[0];
      convertBuffer[0] += p;
      currentBuffer[1] = Double.longBitsToDouble(convertBuffer[0]);
      firePred.train(Double.doubleToLongBits(preValue), convertBuffer[0], e);
      for (int i = 1; i < Block_size; i++) {
        long pred = firePred.predict(convertBuffer[i - 1]);
        long err = convertBuffer[i];
        convertBuffer[i] += pred;
        currentBuffer[i + 1] = Double.longBitsToDouble(convertBuffer[i]);
        firePred.train(convertBuffer[i - 1], convertBuffer[i], err);
      }
    } else {
      throw new UnsupportedOperationException("Sprintz predictive method {} is not supported.");
    }
  }

  @Override
  public double readDouble(ByteBuffer buffer) {
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
