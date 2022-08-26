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
package org.apache.iotdb.tsfile.read.filter.factory;

import org.apache.iotdb.tsfile.read.filter.GroupByFilter;
import org.apache.iotdb.tsfile.read.filter.GroupByMonthFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.read.filter.operator.Between;
import org.apache.iotdb.tsfile.read.filter.operator.Eq;
import org.apache.iotdb.tsfile.read.filter.operator.Gt;
import org.apache.iotdb.tsfile.read.filter.operator.GtEq;
import org.apache.iotdb.tsfile.read.filter.operator.In;
import org.apache.iotdb.tsfile.read.filter.operator.Lt;
import org.apache.iotdb.tsfile.read.filter.operator.LtEq;
import org.apache.iotdb.tsfile.read.filter.operator.NotEq;
import org.apache.iotdb.tsfile.read.filter.operator.NotFilter;
import org.apache.iotdb.tsfile.read.filter.operator.OrFilter;
import org.apache.iotdb.tsfile.read.filter.operator.Regexp;

import java.nio.ByteBuffer;

public class FilterFactory {

  public static AndFilter and(Filter left, Filter right) {
    return new AndFilter(left, right);
  }

  public static OrFilter or(Filter left, Filter right) {
    return new OrFilter(left, right);
  }

  public static NotFilter not(Filter filter) {
    return new NotFilter(filter);
  }

  public static Filter deserialize(ByteBuffer buffer) {
    FilterSerializeId id = FilterSerializeId.values()[buffer.get()];

    Filter filter;
    switch (id) {
      case EQ:
        filter = new Eq<>();
        break;
      case GT:
        filter = new Gt<>();
        break;
      case LT:
        filter = new Lt<>();
        break;
      case OR:
        filter = new OrFilter();
        break;
      case AND:
        filter = new AndFilter();
        break;
      case NEQ:
        filter = new NotEq<>();
        break;
      case NOT:
        filter = new NotFilter();
        break;
      case GTEQ:
        filter = new GtEq<>();
        break;
      case LTEQ:
        filter = new LtEq<>();
        break;
      case BETWEEN:
        filter = new Between<>();
        break;
      case IN:
        filter = new In<>();
        break;
      case GROUP_BY:
        filter = new GroupByFilter();
        break;
      case GROUP_BY_MONTH:
        filter = new GroupByMonthFilter();
        break;
      case REGEXP:
        filter = new Regexp<>();
        break;
      default:
        throw new UnsupportedOperationException("Unknown filter type " + id);
    }
    filter.deserialize(buffer);
    return filter;
  }
}
