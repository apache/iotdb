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

  private List<Binary> map;
  private IntRleDecoder valueDecoder;

  public DictionaryDecoder() {
    super(TSEncoding.PLAIN_DICTIONARY);

    valueDecoder = new IntRleDecoder();
  }

  @Override
  public boolean hasNext(ByteBuffer buffer) {
    if (map == null) {
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
    if (map == null) {
      initMap(buffer);
    }
    int code = valueDecoder.readInt(buffer);
    System.out.println("----");
    System.out.println(code);
    return map.get(code);
  }

  private void initMap(ByteBuffer buffer) {
    map = new ArrayList<>();
    int length = ReadWriteForEncodingUtils.readVarInt(buffer);
    for (int i = 0; i < length; i++) {
      int binaryLength = ReadWriteForEncodingUtils.readVarInt(buffer);
      byte[] buf = new byte[binaryLength];
      buffer.get(buf, 0, binaryLength);
      map.add(new Binary(buf));
    }
  }

  @Override
  public void reset() {
    map = null;
    valueDecoder.reset();
  }
}
