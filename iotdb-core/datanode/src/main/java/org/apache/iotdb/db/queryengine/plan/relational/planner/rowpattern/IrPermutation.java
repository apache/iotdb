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

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class IrPermutation extends IrRowPattern {
  private final List<IrRowPattern> patterns;

  public IrPermutation(List<IrRowPattern> patterns) {
    this.patterns = requireNonNull(patterns, "patterns is null");
    checkArgument(!patterns.isEmpty(), "patterns list is empty");
  }

  public List<IrRowPattern> getPatterns() {
    return patterns;
  }

  @Override
  public <R, C> R accept(IrRowPatternVisitor<R, C> visitor, C context) {
    return visitor.visitIrPermutation(this, context);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    IrPermutation o = (IrPermutation) obj;
    return Objects.equals(patterns, o.patterns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(patterns);
  }

  @Override
  public String toString() {
    return patterns.stream().map(Object::toString).collect(joining(", ", "PERMUTE(", ")"));
  }

  public static void serialize(IrPermutation pattern, ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(pattern.patterns.size(), byteBuffer);
    for (IrRowPattern subPattern : pattern.patterns) {
      IrRowPattern.serialize(subPattern, byteBuffer);
    }
  }

  public static void serialize(IrPermutation pattern, DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(pattern.patterns.size(), stream);
    for (IrRowPattern subPattern : pattern.patterns) {
      IrRowPattern.serialize(subPattern, stream);
    }
  }

  public static IrPermutation deserialize(ByteBuffer byteBuffer) {
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    List<IrRowPattern> patterns = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      patterns.add(IrRowPattern.deserialize(byteBuffer));
    }
    return new IrPermutation(patterns);
  }
}
