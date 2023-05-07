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

package org.apache.iotdb.db.mpp.plan.statement.component;

import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class SortItem {

  private String sortKey;
  private final Ordering ordering;
  private final NullOrdering nullOrdering;

  // only used in analyzing period
  private Expression expression;

  public SortItem(String sortKey, Ordering ordering) {
    this(sortKey, ordering, NullOrdering.LAST);
  }

  public SortItem(String sortKey, Ordering ordering, NullOrdering nullOrdering) {
    this.sortKey = sortKey;
    this.ordering = ordering;
    this.nullOrdering = nullOrdering;
  }

  public SortItem(Expression expression, Ordering ordering, NullOrdering nullOrdering) {
    this.expression = expression;
    this.ordering = ordering;
    this.nullOrdering = nullOrdering;
    this.sortKey = expression.getExpressionString();
  }

  public String getSortKey() {
    return sortKey;
  }

  public Ordering getOrdering() {
    return ordering;
  }

  public NullOrdering getNullOrdering() {
    return nullOrdering;
  }

  public void setExpression(Expression expression) {
    this.expression = expression;
    this.sortKey = expression.getExpressionString();
  }

  public Expression getExpression() {
    return expression;
  }

  public boolean isExpression() {
    return expression != null;
  }

  public SortItem reverse() {
    return new SortItem(getSortKey(), getOrdering().reverse(), getNullOrdering().reverse());
  }

  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(sortKey, byteBuffer);
    ReadWriteIOUtils.write(ordering.ordinal(), byteBuffer);
    ReadWriteIOUtils.write(nullOrdering.ordinal(), byteBuffer);
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(sortKey, stream);
    ReadWriteIOUtils.write(ordering.ordinal(), stream);
    ReadWriteIOUtils.write(nullOrdering.ordinal(), stream);
  }

  public static SortItem deserialize(ByteBuffer byteBuffer) {
    String sortKey = ReadWriteIOUtils.readString(byteBuffer);
    Ordering ordering = Ordering.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    NullOrdering nullOrdering = NullOrdering.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    return new SortItem(sortKey, ordering, nullOrdering);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SortItem sortItem = (SortItem) o;
    return Objects.equals(sortKey, sortItem.sortKey)
        && ordering == sortItem.ordering
        && nullOrdering == sortItem.nullOrdering;
  }

  @Override
  public int hashCode() {
    return Objects.hash(sortKey, ordering, nullOrdering);
  }

  public String toSQLString() {
    return getSortKey() + " " + getOrdering().toString() + " " + getNullOrdering().toString();
  }
}
