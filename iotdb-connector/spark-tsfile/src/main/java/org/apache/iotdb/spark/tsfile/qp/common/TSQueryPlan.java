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
package org.apache.iotdb.spark.tsfile.qp.common;

import java.util.ArrayList;
import java.util.List;

/** One tsfile logical query plan that can be performed at one time */
public class TSQueryPlan {

  private List<String> paths = new ArrayList<>();
  private FilterOperator timeFilterOperator;
  private FilterOperator valueFilterOperator;

  public TSQueryPlan(List<String> paths, FilterOperator timeFilter, FilterOperator valueFilter) {
    this.paths = paths;
    this.timeFilterOperator = timeFilter;
    this.valueFilterOperator = valueFilter;
  }

  public List<String> getPaths() {
    return paths;
  }

  public FilterOperator getTimeFilterOperator() {
    return timeFilterOperator;
  }

  public void setTimeFilterOperator(FilterOperator timeFilterOperator) {
    this.timeFilterOperator = timeFilterOperator;
  }

  public void setValueFilterOperator(FilterOperator valueFilterOperator) {
    this.valueFilterOperator = valueFilterOperator;
  }

  public FilterOperator getValueFilterOperator() {
    return valueFilterOperator;
  }

  public String toString() {
    String ret = "";
    ret += paths.toString();
    if (timeFilterOperator != null) {
      ret += timeFilterOperator.toString();
    }
    if (valueFilterOperator != null) {
      ret += valueFilterOperator.toString();
    }
    return ret;
  }
}
