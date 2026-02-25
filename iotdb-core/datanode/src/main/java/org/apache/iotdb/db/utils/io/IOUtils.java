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

package org.apache.iotdb.db.utils.io;

import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

// TODO: move to TsFile
public class IOUtils {

  private IOUtils() {
    // util class
  }

  public static long writeList(List<? extends BufferSerializable> list, ByteBuffer byteBuffer) {
    long size = ReadWriteForEncodingUtils.writeVarInt(list.size(), byteBuffer);
    for (BufferSerializable item : list) {
      size += item.serialize(byteBuffer);
    }
    return size;
  }

  public static long writeList(List<? extends StreamSerializable> list, OutputStream stream)
      throws IOException {
    long size = ReadWriteForEncodingUtils.writeVarInt(list.size(), stream);
    for (StreamSerializable item : list) {
      size += item.serialize(stream);
    }
    return size;
  }

  public static long writeStringMap(Map<String, String> map, ByteBuffer byteBuffer) {
    long size = ReadWriteForEncodingUtils.writeVarInt(map.size(), byteBuffer);
    for (Entry<String, String> entry : map.entrySet()) {
      size += ReadWriteIOUtils.writeVar(entry.getKey(), byteBuffer);
      size += ReadWriteIOUtils.writeVar(entry.getValue(), byteBuffer);
    }
    return size;
  }

  public static long writeStringMap(Map<String, String> map, OutputStream stream)
      throws IOException {
    long size = ReadWriteForEncodingUtils.writeVarInt(map.size(), stream);
    for (Entry<String, String> entry : map.entrySet()) {
      size += ReadWriteIOUtils.writeVar(entry.getKey(), stream);
      size += ReadWriteIOUtils.writeVar(entry.getValue(), stream);
    }
    return size;
  }

  public static Map<String, String> readLinkedStringMap(ByteBuffer byteBuffer) {
    int size = ReadWriteForEncodingUtils.readVarInt(byteBuffer);
    Map<String, String> map = new LinkedHashMap<>(size);
    for (int i = 0; i < size; i++) {
      map.put(
          ReadWriteIOUtils.readVarIntString(byteBuffer),
          ReadWriteIOUtils.readVarIntString(byteBuffer));
    }
    return map;
  }

  public static Map<String, String> readLinkedStringMap(InputStream stream) throws IOException {
    int size = ReadWriteForEncodingUtils.readVarInt(stream);
    Map<String, String> map = new LinkedHashMap<>(size);
    for (int i = 0; i < size; i++) {
      map.put(ReadWriteIOUtils.readVarIntString(stream), ReadWriteIOUtils.readVarIntString(stream));
    }
    return map;
  }
}
