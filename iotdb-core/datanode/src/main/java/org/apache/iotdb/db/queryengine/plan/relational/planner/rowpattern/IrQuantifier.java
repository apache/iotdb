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
import java.util.Objects;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class IrQuantifier {
  private final int atLeast;
  private final Optional<Integer> atMost;
  private final boolean greedy;

  public static IrQuantifier zeroOrMore(boolean greedy) {
    return new IrQuantifier(0, Optional.empty(), greedy);
  }

  public static IrQuantifier oneOrMore(boolean greedy) {
    return new IrQuantifier(1, Optional.empty(), greedy);
  }

  public static IrQuantifier zeroOrOne(boolean greedy) {
    return new IrQuantifier(0, Optional.of(1), greedy);
  }

  public static IrQuantifier range(
      Optional<Integer> atLeast, Optional<Integer> atMost, boolean greedy) {
    return new IrQuantifier(atLeast.orElse(0), atMost, greedy);
  }

  public IrQuantifier(int atLeast, Optional<Integer> atMost, boolean greedy) {
    this.atLeast = atLeast;
    this.atMost = requireNonNull(atMost, "atMost is null");
    this.greedy = greedy;
  }

  public int getAtLeast() {
    return atLeast;
  }

  public Optional<Integer> getAtMost() {
    return atMost;
  }

  public boolean isGreedy() {
    return greedy;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    IrQuantifier o = (IrQuantifier) obj;
    return atLeast == o.atLeast && Objects.equals(atMost, o.atMost) && greedy == o.greedy;
  }

  @Override
  public int hashCode() {
    return Objects.hash(atLeast, atMost, greedy);
  }

  @Override
  public String toString() {
    return format("{%s, %s}", atLeast, atMost.map(Object::toString).orElse("∞"));
  }

  public static void serialize(IrQuantifier quantifier, ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(quantifier.atLeast, byteBuffer);
    if (quantifier.atMost.isPresent()) {
      ReadWriteIOUtils.write(true, byteBuffer);
      ReadWriteIOUtils.write(quantifier.atMost.get(), byteBuffer);
    } else {
      ReadWriteIOUtils.write(false, byteBuffer);
    }
    ReadWriteIOUtils.write(quantifier.greedy, byteBuffer);
  }

  public static void serialize(IrQuantifier quantifier, DataOutputStream stream)
      throws IOException {
    ReadWriteIOUtils.write(quantifier.atLeast, stream);
    if (quantifier.atMost.isPresent()) {
      ReadWriteIOUtils.write(true, stream);
      ReadWriteIOUtils.write(quantifier.atMost.get(), stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
    }
    ReadWriteIOUtils.write(quantifier.greedy, stream);
  }

  public static IrQuantifier deserialize(ByteBuffer byteBuffer) {
    int atLeast = ReadWriteIOUtils.readInt(byteBuffer);
    boolean hasAtMost = ReadWriteIOUtils.readBoolean(byteBuffer);
    Optional<Integer> atMost =
        hasAtMost ? Optional.of(ReadWriteIOUtils.readInt(byteBuffer)) : java.util.Optional.empty();
    boolean greedy = ReadWriteIOUtils.readBoolean(byteBuffer);
    return new IrQuantifier(atLeast, atMost, greedy);
  }
}
