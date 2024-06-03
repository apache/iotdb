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

package org.apache.iotdb.db.queryengine.plan.relational.utils;

import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.OrderBy;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.SortItem;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

public class NodeUtils {
  private NodeUtils() {}

  public static List<SortItem> getSortItemsFromOrderBy(Optional<OrderBy> orderBy) {
    return orderBy.map(OrderBy::getSortItems).orElse(ImmutableList.of());
  }
}
