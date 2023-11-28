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

public class FilterFactory {

  private FilterFactory() {
    // forbidden construction
  }

  public static And and(Filter left, Filter right) {
    return new And(left, right);
  }

  public static Or or(Filter left, Filter right) {
    return new Or(left, right);
  }

  public static Not not(Filter filter) {
    return new Not(filter);
  }
}
