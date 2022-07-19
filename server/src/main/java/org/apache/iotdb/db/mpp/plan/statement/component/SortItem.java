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

import org.apache.iotdb.db.mpp.plan.statement.StatementNode;

import java.util.Objects;

public class SortItem extends StatementNode {

  public enum SortKey {
    TIME,
    TIMESERIES,
    DEVICE
  }

  public enum Ordering {
    ASC,
    DESC;

    public Ordering reverse() {
      if (this == ASC) {
        return DESC;
      } else {
        return ASC;
      }
    }
  }

  private final SortKey sortKey;
  private final Ordering ordering;

  public SortItem(SortKey sortKey, Ordering ordering) {
    this.sortKey = sortKey;
    this.ordering = ordering;
  }

  public SortKey getSortKey() {
    return sortKey;
  }

  public Ordering getOrdering() {
    return ordering;
  }

  public SortItem reverse() {
    return new SortItem(getSortKey(), getOrdering().reverse());
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
    return sortKey == sortItem.sortKey && ordering == sortItem.ordering;
  }

  @Override
  public int hashCode() {
    return Objects.hash(sortKey, ordering);
  }
}
