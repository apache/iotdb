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

package org.apache.tsfile.read.filter;

import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.FilterFactory;
import org.apache.tsfile.read.filter.operator.And;
import org.apache.tsfile.read.filter.operator.Not;
import org.apache.tsfile.read.filter.operator.Or;

public class PredicateRemoveNotRewriter {

  private PredicateRemoveNotRewriter() {
    // forbidden construction
  }

  public static Filter rewrite(Filter filter) {
    return removeNot(filter);
  }

  private static Filter removeNot(Filter filter) {
    if (filter instanceof And) {
      return FilterFactory.and(
          removeNot(((And) filter).getLeft()), removeNot(((And) filter).getRight()));
    } else if (filter instanceof Or) {
      return FilterFactory.or(
          removeNot(((Or) filter).getLeft()), removeNot(((Or) filter).getRight()));
    } else if (filter instanceof Not) {
      return ((Not) filter).getFilter().reverse();
    }
    return filter;
  }
}
