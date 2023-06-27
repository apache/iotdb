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

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * An encoder implementing dictionary encoding.
 *
 * <pre>Encoding format: {@code
 * <map> <indexes>
 * <map> := <map length> <map data>
 * <map data> := [<entry size><entry data>]...
 * <indexes> := [<index>]...
 * }</pre>
 */
public class DictionaryEncoder extends Encoder {
  private static final Logger logger = LoggerFactory.getLogger(DictionaryEncoder.class);

  private HashMap<Binary, Integer> entryIndex;
  private List<Binary> indexEntry;
  private IntRleEncoder valuesEncoder;
  private long mapSize;

  public DictionaryEncoder() {
    super(TSEncoding.DICTIONARY);

    entryIndex = new HashMap<>();
    indexEntry = new ArrayList<>();
    valuesEncoder = new IntRleEncoder();
    mapSize = 0;
  }

  @Override
  public void encode(Binary value, ByteArrayOutputStream out) {
    entryIndex.computeIfAbsent(
        value,
        (v) -> {
          indexEntry.add(v);
          mapSize += v.getLength();
          return entryIndex.size();
        });
    valuesEncoder.encode(entryIndex.get(value), out);
  }

  @Override
  public void flush(ByteArrayOutputStream out) {
    try {
      writeMap(out);
      writeEncodedData(out);
    } catch (IOException e) {
      logger.error("tsfile-encoding DictionaryEncoder: error occurs when flushing", e);
    }
    reset();
  }

  @Override
  public int getOneItemMaxSize() {
    // map + one encoded value = (map size + map value) + one encoded value = (4 + 4) + 4
    return 12;
  }

  @Override
  public long getMaxByteSize() {
    // has max size when when all points are unique
    return 4 + mapSize + valuesEncoder.getMaxByteSize();
  }

  private void writeMap(ByteArrayOutputStream out) throws IOException {
    ReadWriteForEncodingUtils.writeVarInt(indexEntry.size(), out);
    for (Binary value : indexEntry) {
      ReadWriteForEncodingUtils.writeVarInt(value.getLength(), out);
      out.write(value.getValues());
    }
  }

  private void writeEncodedData(ByteArrayOutputStream out) throws IOException {
    valuesEncoder.flush(out);
  }

  private void reset() {
    entryIndex.clear();
    indexEntry.clear();
    valuesEncoder.reset();
    mapSize = 0;
  }
}
