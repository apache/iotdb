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

public class LongSprintzDecoder extends SprintzDecoder {
  LongPacker packer;
  LongFire firePred;
  private long preValue;
  private final long[] currentBuffer;
  private long currentValue;
  private final String predictScheme =
      TSFileDescriptor.getInstance().getConfig().getSprintzPredictScheme();

  public LongSprintzDecoder() {
    super();
    firePred = new LongFire(3);
    currentBuffer = new long[Block_size + 1];
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
  }

  @Override
  protected void decodeBlock(ByteBuffer in) throws IOException {
    bitWidth = ReadWriteForEncodingUtils.readIntLittleEndianPaddedOnBitWidth(in, 1);
    if ((bitWidth & (1 << 7)) != 0) {
      decodeSize = bitWidth & ~(1 << 7);
      LongRleDecoder decoder = new LongRleDecoder();
      for (int i = 0; i < decodeSize; i++) {
        currentBuffer[i] = decoder.readLong(in);
      }
    } else {
      decodeSize = Block_size + 1;
      preValue = in.getLong();
      currentBuffer[0] = preValue;
      long[] tmpBuffer = new long[8];
      packer = new LongPacker(bitWidth);
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
      for (int i = 1; i <= Block_size; i++) {
        long pred = firePred.predict(currentBuffer[i - 1]);
        long err = currentBuffer[i];
        currentBuffer[i] = pred + err;
        firePred.train(currentBuffer[i - 1], currentBuffer[i], err);
      }
    } else {
      throw new UnsupportedOperationException("Sprintz predictive method {} is not supported.");
    }
  }

  @Override
  public long readLong(ByteBuffer buffer) {
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
    return currentValue;
  }
}
