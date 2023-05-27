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

package org.apache.iotdb.commons.consensus.index.impl;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.ProgressIndexType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class IoTProgressIndex implements ProgressIndex {
  private final Map<Integer, Long> peerId2SearchIndex;

  public IoTProgressIndex() {
    peerId2SearchIndex = new HashMap<>();
  }

  public void addSearchIndex(Integer peerId, Long searchIndex) {
    peerId2SearchIndex.put(peerId, searchIndex);
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    ProgressIndexType.IOT_CONSENSUS_INDEX.serialize(byteBuffer);
    // TODO: impl it
    ReadWriteIOUtils.write(peerId2SearchIndex.size(), byteBuffer);
    peerId2SearchIndex.forEach(
        (key, value) -> {
          ReadWriteIOUtils.write(key, byteBuffer);
          ReadWriteIOUtils.write(value, byteBuffer);
        });
  }

  @Override
  public void serialize(OutputStream stream) throws IOException {
    ProgressIndexType.IOT_CONSENSUS_INDEX.serialize(stream);
    // TODO: impl it
    ReadWriteIOUtils.write(peerId2SearchIndex.size(), stream);
    for (Map.Entry<Integer, Long> entry : peerId2SearchIndex.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), stream);
      ReadWriteIOUtils.write(entry.getValue(), stream);
    }
  }

  @Override
  public boolean isAfter(ProgressIndex progressIndex) {
    if (!(progressIndex instanceof IoTProgressIndex)) {
      return false;
    }

    return ((IoTProgressIndex) progressIndex)
        .peerId2SearchIndex.entrySet().stream()
            .noneMatch(
                entry ->
                    !this.peerId2SearchIndex.containsKey(entry.getKey())
                        || this.peerId2SearchIndex.get(entry.getKey()) <= entry.getValue());
  }

  @Override
  public boolean equals(ProgressIndex progressIndex) {
    if (!(progressIndex instanceof IoTProgressIndex)) {
      return false;
    }

    return ((IoTProgressIndex) progressIndex)
        .peerId2SearchIndex.entrySet().stream()
            .allMatch(
                entry ->
                    this.peerId2SearchIndex.containsKey(entry.getKey())
                        && this.peerId2SearchIndex.get(entry.getKey()).equals(entry.getValue()));
  }

  @Override
  public ProgressIndex updateToMaximum(ProgressIndex progressIndex) {
    if (!(progressIndex instanceof IoTProgressIndex)) {
      return this;
    }

    ((IoTProgressIndex) progressIndex)
        .peerId2SearchIndex.forEach(
            (thatK, thatV) ->
                this.peerId2SearchIndex.compute(
                    thatK, (thisK, thisV) -> (thisV == null ? thatV : Math.max(thisV, thatV))));
    return this;
  }

  public static IoTProgressIndex deserializeFrom(ByteBuffer byteBuffer) {
    // TODO: impl it
    IoTProgressIndex ioTProgressIndex = new IoTProgressIndex();
    int num = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < num; i++) {
      ioTProgressIndex.addSearchIndex(
          ReadWriteIOUtils.readInt(byteBuffer), ReadWriteIOUtils.readLong(byteBuffer));
    }
    return ioTProgressIndex;
  }

  public static IoTProgressIndex deserializeFrom(InputStream stream) throws IOException {
    // TODO: impl it
    IoTProgressIndex ioTProgressIndex = new IoTProgressIndex();
    int num = ReadWriteIOUtils.readInt(stream);
    for (int i = 0; i < num; i++) {
      ioTProgressIndex.addSearchIndex(
          ReadWriteIOUtils.readInt(stream), ReadWriteIOUtils.readLong(stream));
    }
    return ioTProgressIndex;
  }
}
