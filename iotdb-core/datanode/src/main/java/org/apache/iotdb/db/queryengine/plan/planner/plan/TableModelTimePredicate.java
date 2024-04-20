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

package org.apache.iotdb.db.queryengine.plan.planner.plan;

import org.apache.iotdb.db.queryengine.plan.analyze.PredicateUtils;
import org.apache.iotdb.db.relational.sql.tree.Expression;

import org.apache.tsfile.read.filter.basic.Filter;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Objects;

public class TableModelTimePredicate implements TimePredicate {

  private final Expression timePredicate;

  public TableModelTimePredicate(Expression timePredicate) {
    this.timePredicate = timePredicate;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    throw new UnsupportedEncodingException();
  }

  @Override
  public Filter convertPredicateToTimeFilter() {
    return PredicateUtils.convertPredicateToTimeFilter(timePredicate);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TableModelTimePredicate that = (TableModelTimePredicate) o;
    return Objects.equals(timePredicate, that.timePredicate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(timePredicate);
  }
}
