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
import java.util.List;
import java.util.Objects;

public class TabletsPayload implements SubscriptionPollPayload {

  /** A batch of tablets. */
  private transient List<Tablet> tablets = new ArrayList<>();

  /**
   * The field to be filled in the next {@link PollTabletsPayload} request. If negative, it
   * indicates all tablets have been fetched, and -nextOffset represents the total number of
   * tablets.
   */
  private transient int nextOffset;

  public TabletsPayload() {}

  public TabletsPayload(final List<Tablet> tablets, final int nextOffset) {
    this.tablets = tablets;
    this.nextOffset = nextOffset;
  }

  public List<Tablet> getTablets() {
    return tablets;
  }

  public int getNextOffset() {
    return nextOffset;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(tablets.size(), stream);
    for (final Tablet tablet : tablets) {
      tablet.serialize(stream);
    }
    ReadWriteIOUtils.write(nextOffset, stream);
  }

  @Override
  public SubscriptionPollPayload deserialize(final ByteBuffer buffer) {
    final List<Tablet> tablets = new ArrayList<>();
    final int size = ReadWriteIOUtils.readInt(buffer);
    for (int i = 0; i < size; ++i) {
      tablets.add(Tablet.deserialize(buffer));
    }
    this.tablets = tablets;
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
