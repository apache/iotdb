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

public class IntSprintzDecoder extends SprintzDecoder {

  IntPacker packer;
  IntFire firePred;
  private int preValue;
  private final int[] currentBuffer;
  private int currentValue;
  private final String predictScheme =
      TSFileDescriptor.getInstance().getConfig().getSprintzPredictScheme();

  public IntSprintzDecoder() {
    super();
    currentBuffer = new int[Block_size + 1];
    firePred = new IntFire(2);
    reset();
  }

  @Override
  public boolean hasNext(ByteBuffer buffer) throws IOException {
    int minLenth = Integer.BYTES + 1;
    return (isBlockReaded && currentCount < Block_size) || buffer.remaining() >= minLenth;
  }

  @Override
  public void reset() {
    super.reset();
    currentValue = 0;
    preValue = 0;
    currentCount = 0;
    Arrays.fill(currentBuffer, 0);
  }

  @Override
  protected void decodeBlock(ByteBuffer in) throws IOException {
    bitWidth = ReadWriteForEncodingUtils.readIntLittleEndianPaddedOnBitWidth(in, 1);
    if ((bitWidth & (1 << 7)) != 0) {
      decodeSize = bitWidth & ~(1 << 7);
      IntRleDecoder decoder = new IntRleDecoder();
      for (int i = 0; i < decodeSize; i++) {
        currentBuffer[i] = decoder.readInt(in);
      }
    } else {
      decodeSize = Block_size + 1;
      preValue = ReadWriteForEncodingUtils.readUnsignedVarInt(in);
      currentBuffer[0] = preValue;
      int[] tmpBuffer = new int[8];
      packer = new IntPacker(bitWidth);
      byte[] packcle = new byte[bitWidth];
      for (int i = 0; i < bitWidth; i++) {
        packcle[i] = in.get();
      }
      packer.unpack8Values(packcle, 0, tmpBuffer);
      for (int i = 0; i < 8; i++) currentBuffer[i + 1] = tmpBuffer[i];
      recalculate();
    }
    isBlockReaded = true;
  }

  @Override
  protected void recalculate() {
    for (int i = 1; i <= Block_size; i++) {
      if (currentBuffer[i] % 2 == 0) currentBuffer[i] = -currentBuffer[i] / 2;
      else currentBuffer[i] = (currentBuffer[i] + 1) / 2;
    }
    if (predictScheme.equals("delta")) {
      for (int i = 1; i < currentBuffer.length; i++) {
        currentBuffer[i] += currentBuffer[i - 1];
      }
    } else if (predictScheme.equals("fire")) {
      firePred.reset();
      int pre = currentBuffer[0];
      for (int i = 1; i <= Block_size; i++) {
        int pred = firePred.predict(currentBuffer[i - 1]);
        int err = currentBuffer[i];
        currentBuffer[i] = pred + err;
        firePred.train(currentBuffer[i - 1], currentBuffer[i], err);
      }
    } else {
      throw new UnsupportedOperationException("Sprintz predictive method {} is not supported.");
    }
  }

  @Override
  public int readInt(ByteBuffer buffer) {
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
