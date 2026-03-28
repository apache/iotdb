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

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.record.Tablet;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class TabletsPayload implements SubscriptionPollPayload {

  /** A batch of tablets. */
  private transient Map<String, List<Tablet>> tablets;

  /**
   * The field to be filled in the next {@link PollTabletsPayload} request.
   *
   * <ul>
   *   <li>If nextOffset is 1, it indicates that the current payload is the first payload (its
   *       tablets are empty) and the fetching should continue.
   *   <li>If nextOffset is negative (or zero), it indicates all tablets have been fetched, and
   *       -nextOffset represents the total number of tablets.
   * </ul>
   */
  private transient int nextOffset;

  public TabletsPayload() {}

  public TabletsPayload(final List<Tablet> tablets, final int nextOffset) {
    if (tablets.isEmpty()) {
      this.tablets = Collections.emptyMap();
    } else {
      this.tablets = Collections.singletonMap(null, tablets);
    }
    this.nextOffset = nextOffset;
  }

  public TabletsPayload(final Map<String, List<Tablet>> tablets, final int nextOffset) {
    this.tablets = tablets;
    this.nextOffset = nextOffset;
  }

  public List<Tablet> getTablets() {
    return tablets.values().stream().flatMap(List::stream).collect(Collectors.toList());
  }

  public Map<String, List<Tablet>> getTabletsWithDBInfo() {
    return tablets;
  }

  public int getNextOffset() {
    return nextOffset;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(tablets.size(), stream);
    for (Map.Entry<String, List<Tablet>> entry : tablets.entrySet()) {
      final String databaseName = entry.getKey();
      final List<Tablet> tabletList = entry.getValue();
      ReadWriteIOUtils.write(databaseName, stream);
      ReadWriteIOUtils.write(tabletList.size(), stream);
      for (final Tablet tablet : tabletList) {
        tablet.serialize(stream);
      }
    }
    ReadWriteIOUtils.write(nextOffset, stream);
  }

  @Override
  public SubscriptionPollPayload deserialize(final ByteBuffer buffer) {
    final Map<String, List<Tablet>> tabletsWithDBInfo = new HashMap<>();
    final int size = ReadWriteIOUtils.readInt(buffer);
    for (int i = 0; i < size; ++i) {
      final String databaseName = ReadWriteIOUtils.readString(buffer);
      final int tabletsSize = ReadWriteIOUtils.readInt(buffer);
      final List<Tablet> tablets = new ArrayList<>();
      for (int j = 0; j < tabletsSize; ++j) {
        tablets.add(Tablet.deserialize(buffer));
      }
      tabletsWithDBInfo.put(databaseName, tablets);
    }
    this.tablets = tabletsWithDBInfo;
    this.nextOffset = ReadWriteIOUtils.readInt(buffer);
    return this;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final TabletsPayload that = (TabletsPayload) obj;
    return Objects.equals(this.tablets, that.tablets)
        && Objects.equals(this.nextOffset, that.nextOffset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tablets, nextOffset);
  }

  @Override
  public String toString() {
    return "TabletsPayload{size of tablets=" + tablets.size() + ", nextOffset=" + nextOffset + "}";
  }
}
