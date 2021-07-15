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
 *
 */
package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

public class ShowTimeSeriesOperator extends ShowOperator {

  private PartialPath path;
  private boolean isContains;
  private String key;
  private String value;
  private int limit = 0;
  private int offset = 0;
  // if is true, the result will be sorted according to the inserting frequency of the timeseries
  private final boolean orderByHeat;

  public ShowTimeSeriesOperator(int tokeIntType, PartialPath path, boolean orderByHeat) {
    super(tokeIntType);
    this.path = path;
    this.orderByHeat = orderByHeat;
  }

  public PartialPath getPath() {
    return path;
  }

  public boolean isContains() {
    return isContains;
  }

  public void setContains(boolean contains) {
    isContains = contains;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public int getLimit() {
    return limit;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  public int getOffset() {
    return offset;
  }

  public void setOffset(int offset) {
    this.offset = offset;
  }

  public boolean isOrderByHeat() {
    return orderByHeat;
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    ShowTimeSeriesPlan showTimeSeriesPlan = new ShowTimeSeriesPlan(path, limit, offset);
    showTimeSeriesPlan.setIsContains(isContains);
    showTimeSeriesPlan.setKey(key);
    showTimeSeriesPlan.setValue(value);
    showTimeSeriesPlan.setOrderByHeat(orderByHeat);
    return showTimeSeriesPlan;
  }
}
