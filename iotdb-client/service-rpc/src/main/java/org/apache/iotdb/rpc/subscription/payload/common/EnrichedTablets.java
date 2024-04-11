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

package org.apache.iotdb.rpc.subscription.payload.common;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.record.Tablet;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class EnrichedTablets {

  private final transient SubscriptionCommitContext commitContext;
  private final transient List<Tablet> tablets;

  public List<Tablet> getTablets() {
    return tablets;
  }

  public EnrichedTablets(
      final SubscriptionCommitContext commitContext, final List<Tablet> tablets) {
    this.commitContext = commitContext;
    this.tablets = tablets;
  }

  /////////////////////////////// de/ser ///////////////////////////////

  public void serialize(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(tablets.size(), stream);
    for (final Tablet tablet : tablets) {
      tablet.serialize(stream);
    }
  }

  public static EnrichedTablets deserialize(final ByteBuffer buffer) {
    final SubscriptionCommitContext commitContext = SubscriptionCommitContext.deserialize(buffer);
    final List<Tablet> tablets = new ArrayList<>();
    final int size = ReadWriteIOUtils.readInt(buffer);
    for (int i = 0; i < size; ++i) {
      tablets.add(Tablet.deserialize(buffer));
    }
    return new EnrichedTablets(commitContext, tablets);
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final EnrichedTablets that = (EnrichedTablets) obj;
    return Objects.equals(this.commitContext, that.commitContext)
        && Objects.equals(this.tablets, that.tablets);
  }

  @Override
  public int hashCode() {
    // Considering that the Tablet class has not implemented the hashCode method, the tablets member
    // should not be included when calculating the hashCode of EnrichedTablets.
    return Objects.hash(commitContext);
  }
}
