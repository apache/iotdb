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

package org.apache.iotdb.tsfile.read.filter;

import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.read.filter.factory.FilterSerializeId;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.read.filter.operator.NotFilter;
import org.apache.iotdb.tsfile.read.filter.operator.OrFilter;

public class PredicateRemoveNotRewriter {

  private PredicateRemoveNotRewriter() {
    // forbidden to construct
  }

  public static Filter rewrite(Filter filter) {
    return removeNot(filter);
  }

  private static Filter removeNot(Filter filter) {
    FilterSerializeId filterType = filter.getSerializeId();
    switch (filterType) {
      case AND:
        return FilterFactory.and(
            removeNot(((AndFilter) filter).getLeft()), removeNot(((AndFilter) filter).getRight()));
      case OR:
        return FilterFactory.or(
            removeNot(((OrFilter) filter).getLeft()), removeNot(((OrFilter) filter).getRight()));
      case NOT:
        return ((NotFilter) filter).getFilter().reverse();
      default:
        return filter;
    }
  }
}
