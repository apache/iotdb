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

package org.apache.iotdb.rpc.subscription.payload.poll;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class TopicProgress {

  private final Map<String, RegionProgress> regionProgress;

  public TopicProgress(final Map<String, RegionProgress> regionProgress) {
    this.regionProgress =
        regionProgress == null
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(new LinkedHashMap<>(regionProgress));
  }

  public Map<String, RegionProgress> getRegionProgress() {
    return regionProgress;
  }

  public static ByteBuffer serialize(final TopicProgress progress) throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      progress.serialize(outputStream);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
  }

  public void serialize(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(regionProgress.size(), stream);
    for (final Map.Entry<String, RegionProgress> entry : regionProgress.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), stream);
      entry.getValue().serialize(stream);
    }
  }

  public static TopicProgress deserialize(final ByteBuffer buffer) {
    final int size = ReadWriteIOUtils.readInt(buffer);
    final Map<String, RegionProgress> regionProgress = new LinkedHashMap<>(size);
    for (int i = 0; i < size; i++) {
      regionProgress.put(ReadWriteIOUtils.readString(buffer), RegionProgress.deserialize(buffer));
    }
    return new TopicProgress(regionProgress);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof TopicProgress)) {
      return false;
    }
    final TopicProgress that = (TopicProgress) obj;
    return Objects.equals(regionProgress, that.regionProgress);
  }

  @Override
  public int hashCode() {
    return Objects.hash(regionProgress);
  }

  @Override
  public String toString() {
    return "TopicProgress{" + "regionProgress=" + regionProgress + '}';
  }
}
