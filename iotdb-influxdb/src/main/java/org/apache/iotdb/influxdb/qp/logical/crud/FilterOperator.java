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

package org.apache.iotdb.influxdb.qp.logical.crud;

import org.apache.iotdb.influxdb.qp.constant.FilterConstant;

import java.util.ArrayList;
import java.util.List;

public class FilterOperator implements Comparable<FilterOperator> {

  public FilterConstant.FilterType getFilterType() {
    return filterType;
  }

  public void setFilterType(FilterConstant.FilterType filterType) {
    this.filterType = filterType;
  }

  public List<FilterOperator> getChildOperators() {
    return childOperators;
  }

  public void setChildOperators(List<FilterOperator> childOperators) {
    this.childOperators = childOperators;
  }

  public String getKeyName() {
    return keyName;
  }

  public void setKeyName(String keyName) {
    this.keyName = keyName;
  }

  protected FilterConstant.FilterType filterType;

  private List<FilterOperator> childOperators = new ArrayList<>();

  String keyName;

  public FilterOperator() {}

  public FilterOperator(FilterConstant.FilterType filterType) {
    this.filterType = filterType;
  }

  public List<FilterOperator> getChildren() {
    return childOperators;
  }

  public boolean addChildOperator(FilterOperator op) {
    childOperators.add(op);
    return true;
  }

  @Override
  public int compareTo(FilterOperator o) {
    return 0;
  }
}
