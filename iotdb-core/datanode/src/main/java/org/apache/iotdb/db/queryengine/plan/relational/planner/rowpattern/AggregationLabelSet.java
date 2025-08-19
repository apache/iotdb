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

package org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class AggregationLabelSet {
  // A set of labels to identify all rows to aggregate over:
  // avg(A.price) => this is an aggregation over rows with label A, so labels = {A}
  // avg(Union.price) => this is an aggregation over rows matching a union variable Union, so for
  // SUBSET Union = (A, B, C), labels = {A, B, C}
  // avg(price) => this is an aggregation over "universal pattern variable", which is effectively
  // over all rows, no matter the assigned labels. In such case labels = {}
  private final Set<IrLabel> labels;

  private final boolean running;

  public AggregationLabelSet(Set<IrLabel> labels, boolean running) {
    this.labels = requireNonNull(labels, "labels is null");
    this.running = running;
  }

  public Set<IrLabel> getLabels() {
    return labels;
  }

  public boolean isRunning() {
    return running;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AggregationLabelSet that = (AggregationLabelSet) o;
    return labels.equals(that.labels) && running == that.running;
  }

  @Override
  public int hashCode() {
    return Objects.hash(labels, running);
  }

  public static void serialize(AggregationLabelSet set, ByteBuffer byteBuffer) {
    byteBuffer.putInt(set.labels.size());
    for (IrLabel label : set.labels) {
      IrLabel.serialize(label, byteBuffer);
    }
    byteBuffer.put((byte) (set.running ? 1 : 0));
  }

  public static void serialize(AggregationLabelSet set, DataOutputStream stream)
      throws IOException {
    stream.writeInt(set.labels.size());
    for (IrLabel label : set.labels) {
      IrLabel.serialize(label, stream);
    }
    stream.writeBoolean(set.running);
  }

  public static AggregationLabelSet deserialize(ByteBuffer byteBuffer) {
    int size = byteBuffer.getInt();
    Set<IrLabel> labels = new HashSet<>(size);
    for (int i = 0; i < size; i++) {
      labels.add(IrLabel.deserialize(byteBuffer));
    }
    boolean running = byteBuffer.get() == 1;
    return new AggregationLabelSet(labels, running);
  }
}
