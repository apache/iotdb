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

import org.apache.iotdb.db.queryengine.plan.expression.Expression;

import org.apache.tsfile.read.filter.basic.Filter;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface TimePredicate {

  void serialize(DataOutputStream stream) throws IOException;

  Filter convertPredicateToTimeFilter();

  static TimePredicate deserialize(ByteBuffer byteBuffer) {
    // TODO will return another kind of TimePredicate like TableModelTimePredicate in the future
    return new TreeModelTimePredicate(Expression.deserialize(byteBuffer));
  }
}
