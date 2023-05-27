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

    ReadWriteIOUtils.write(peerId2SearchIndex.size(), byteBuffer);
    for (final Map.Entry<Integer, Long> entry : peerId2SearchIndex.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), byteBuffer);
      ReadWriteIOUtils.write(entry.getValue(), byteBuffer);
    }
  }

  @Override
  public void serialize(OutputStream stream) throws IOException {
    ProgressIndexType.IOT_CONSENSUS_INDEX.serialize(stream);

    ReadWriteIOUtils.write(peerId2SearchIndex.size(), stream);
    for (final Map.Entry<Integer, Long> entry : peerId2SearchIndex.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), stream);
      ReadWriteIOUtils.write(entry.getValue(), stream);
    }
  }

  @Override
  public boolean isAfter(ProgressIndex progressIndex) {
    if (progressIndex instanceof MinimumProgressIndex) {
      return true;
    }

    if (!(progressIndex instanceof IoTProgressIndex)) {
      return false;
    }

    final IoTProgressIndex thisIoTProgressIndex = this;
    final IoTProgressIndex thatIoTProgressIndex = (IoTProgressIndex) progressIndex;
    return thatIoTProgressIndex.peerId2SearchIndex.entrySet().stream()
        .noneMatch(
            entry ->
                !thisIoTProgressIndex.peerId2SearchIndex.containsKey(entry.getKey())
                    || thisIoTProgressIndex.peerId2SearchIndex.get(entry.getKey())
                        <= entry.getValue());
  }

  @Override
  public boolean equals(ProgressIndex progressIndex) {
    if (!(progressIndex instanceof IoTProgressIndex)) {
      return false;
    }

    final IoTProgressIndex thisIoTProgressIndex = this;
    final IoTProgressIndex thatIoTProgressIndex = (IoTProgressIndex) progressIndex;
    return thisIoTProgressIndex.peerId2SearchIndex.size()
            == thatIoTProgressIndex.peerId2SearchIndex.size()
        && thatIoTProgressIndex.peerId2SearchIndex.entrySet().stream()
            .allMatch(
                entry ->
                    thisIoTProgressIndex.peerId2SearchIndex.containsKey(entry.getKey())
                        && thisIoTProgressIndex
                            .peerId2SearchIndex
                            .get(entry.getKey())
                            .equals(entry.getValue()));
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof IoTProgressIndex)) {
      return false;
    }
    return this.equals((IoTProgressIndex) obj);
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public ProgressIndex updateToMinimumIsAfterProgressIndex(ProgressIndex progressIndex) {
    if (!(progressIndex instanceof IoTProgressIndex)) {
      return this;
    }

    final IoTProgressIndex thisIoTProgressIndex = this;
    final IoTProgressIndex thatIoTProgressIndex = (IoTProgressIndex) progressIndex;
    thatIoTProgressIndex.peerId2SearchIndex.forEach(
        (thatK, thatV) ->
            thisIoTProgressIndex.peerId2SearchIndex.compute(
                thatK, (thisK, thisV) -> (thisV == null ? thatV : Math.max(thisV, thatV))));
    return this;
  }

  public static IoTProgressIndex deserializeFrom(ByteBuffer byteBuffer) {
    final IoTProgressIndex ioTProgressIndex = new IoTProgressIndex();
    final int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; i++) {
      final int peerId = ReadWriteIOUtils.readInt(byteBuffer);
      final long searchIndex = ReadWriteIOUtils.readLong(byteBuffer);
      ioTProgressIndex.addSearchIndex(peerId, searchIndex);
    }
    return ioTProgressIndex;
  }

  public static IoTProgressIndex deserializeFrom(InputStream stream) throws IOException {
    final IoTProgressIndex ioTProgressIndex = new IoTProgressIndex();
    final int size = ReadWriteIOUtils.readInt(stream);
    for (int i = 0; i < size; i++) {
      final int peerId = ReadWriteIOUtils.readInt(stream);
      final long searchIndex = ReadWriteIOUtils.readLong(stream);
      ioTProgressIndex.addSearchIndex(peerId, searchIndex);
    }
    return ioTProgressIndex;
  }

  @Override
  public String toString() {
    return "IoTProgressIndex{" + "peerId2SearchIndex=" + peerId2SearchIndex + '}';
  }
}
