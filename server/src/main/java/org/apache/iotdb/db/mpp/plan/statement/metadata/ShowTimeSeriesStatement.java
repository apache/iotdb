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

package org.apache.iotdb.db.mpp.plan.statement.metadata;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;

import java.util.Collections;
import java.util.List;

/**
 * SHOW TIMESERIES statement.
 *
 * <p>Here is the syntax definition:
 *
 * <p>SHOW [LATEST] TIMESERIES [pathPattern] [WHERE key { = | CONTAINS } value] [LIMIT limit]
 * [OFFSET offset]
 */
public class ShowTimeSeriesStatement extends ShowStatement {

  private final PartialPath pathPattern;

  private boolean isContains;

  private String key;
  private String value;

  // if is true, the result will be sorted according to the inserting frequency of the time series
  private final boolean orderByHeat;

  public ShowTimeSeriesStatement(PartialPath pathPattern, boolean orderByHeat) {
    super();
    this.pathPattern = pathPattern;
    this.orderByHeat = orderByHeat;
  }

  public PartialPath getPathPattern() {
    return pathPattern;
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

  public boolean isOrderByHeat() {
    return orderByHeat;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.singletonList(pathPattern);
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitShowTimeSeries(this, context);
  }
}
