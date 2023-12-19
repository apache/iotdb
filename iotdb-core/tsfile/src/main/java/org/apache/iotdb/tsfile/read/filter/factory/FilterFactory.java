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

import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.And;
import org.apache.iotdb.tsfile.read.filter.operator.Not;
import org.apache.iotdb.tsfile.read.filter.operator.Or;

import static org.apache.iotdb.tsfile.utils.Preconditions.checkArgument;

public class FilterFactory {

  private FilterFactory() {
    // forbidden construction
  }

  public static Filter and(Filter left, Filter right) {
    if (left == null && right == null) {
      return null;
    } else if (left == null) {
      return right;
    } else if (right == null) {
      return left;
    }
    return new And(left, right);
  }

  public static Filter or(Filter left, Filter right) {
    if (left == null && right == null) {
      return null;
    } else if (left == null) {
      return right;
    } else if (right == null) {
      return left;
    }
    return new Or(left, right);
  }

  public static Not not(Filter filter) {
    checkArgument(filter != null, "filter cannot be null");
    return new Not(filter);
  }
}
