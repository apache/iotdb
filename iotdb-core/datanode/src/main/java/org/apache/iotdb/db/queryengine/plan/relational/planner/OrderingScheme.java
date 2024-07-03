/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class OrderingScheme {
  private final List<Symbol> orderBy;
  private final Map<Symbol, SortOrder> orderings;

  public OrderingScheme(List<Symbol> orderBy, Map<Symbol, SortOrder> orderings) {
    requireNonNull(orderBy, "orderBy is null");
    requireNonNull(orderings, "orderings is null");
    checkArgument(!orderBy.isEmpty(), "orderBy is empty");
    checkArgument(
        orderings.keySet().equals(ImmutableSet.copyOf(orderBy)),
        "orderBy keys and orderings don't match");
    this.orderBy = ImmutableList.copyOf(orderBy);
    this.orderings = ImmutableMap.copyOf(orderings);
  }

  public List<Symbol> getOrderBy() {
    return orderBy;
  }

  public Map<Symbol, SortOrder> getOrderings() {
    return orderings;
  }

  public List<SortOrder> getOrderingList() {
    return orderBy.stream().map(orderings::get).collect(toImmutableList());
  }

  public SortOrder getOrdering(Symbol symbol) {
    checkArgument(orderings.containsKey(symbol), "No ordering for symbol: %s", symbol);
    return orderings.get(symbol);
  }

  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(orderBy.size(), byteBuffer);
    for (Symbol symbol : orderBy) {
      Symbol.serialize(symbol, byteBuffer);
    }

    ReadWriteIOUtils.write(orderings.size(), byteBuffer);
    for (Map.Entry<Symbol, SortOrder> entry : orderings.entrySet()) {
      Symbol.serialize(entry.getKey(), byteBuffer);
      ReadWriteIOUtils.write(entry.getValue().ordinal(), byteBuffer);
    }
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(orderBy.size(), stream);
    for (Symbol symbol : orderBy) {
      Symbol.serialize(symbol, stream);
    }

    ReadWriteIOUtils.write(orderings.size(), stream);
    for (Map.Entry<Symbol, SortOrder> entry : orderings.entrySet()) {
      Symbol.serialize(entry.getKey(), stream);
      ReadWriteIOUtils.write(entry.getValue().ordinal(), stream);
    }
  }

  public static OrderingScheme deserialize(ByteBuffer byteBuffer) {
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    List<Symbol> orderBy = new ArrayList<>(size);
    while (size-- > 0) {
      orderBy.add(Symbol.deserialize(byteBuffer));
    }

    size = ReadWriteIOUtils.readInt(byteBuffer);
    Map<Symbol, SortOrder> orderings = new HashMap<>(size);
    while (size-- > 0) {
      orderings.put(
          Symbol.deserialize(byteBuffer), SortOrder.values()[ReadWriteIOUtils.readInt(byteBuffer)]);
    }

    return new OrderingScheme(orderBy, orderings);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OrderingScheme that = (OrderingScheme) o;
    return Objects.equals(orderBy, that.orderBy) && Objects.equals(orderings, that.orderings);
  }

  @Override
  public int hashCode() {
    return Objects.hash(orderBy, orderings);
  }

  @Override
  public String toString() {
    return toStringHelper("").add("orderBy", orderBy).add("orderings", orderings).toString();
  }
}
