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

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DictionaryDecoder extends Decoder {
  private static final Logger logger = LoggerFactory.getLogger(DictionaryDecoder.class);

  private List<Binary> entryIndex;
  private IntRleDecoder valueDecoder;

  public DictionaryDecoder() {
    super(TSEncoding.DICTIONARY);

    valueDecoder = new IntRleDecoder();
  }

  @Override
  public boolean hasNext(ByteBuffer buffer) {
    if (entryIndex == null) {
      initMap(buffer);
    }

    try {
      return valueDecoder.hasNext(buffer);
    } catch (IOException e) {
      logger.error("tsfile-decoding DictionaryDecoder: error occurs when decoding", e);
    }

    return false;
  }

  @Override
  public Binary readBinary(ByteBuffer buffer) {
    if (entryIndex == null) {
      initMap(buffer);
    }
    int code = valueDecoder.readInt(buffer);
    return entryIndex.get(code);
  }

  private void initMap(ByteBuffer buffer) {
    int length = ReadWriteForEncodingUtils.readVarInt(buffer);
    entryIndex = new ArrayList<>(length);
    for (int i = 0; i < length; i++) {
      int binaryLength = ReadWriteForEncodingUtils.readVarInt(buffer);
      byte[] buf = new byte[binaryLength];
      buffer.get(buf, 0, binaryLength);
      entryIndex.add(new Binary(buf));
    }
  }

  @Override
  public void reset() {
    entryIndex = null;
    valueDecoder.reset();
  }
}
