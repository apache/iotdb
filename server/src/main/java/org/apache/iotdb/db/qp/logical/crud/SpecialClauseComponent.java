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

package org.apache.iotdb.db.qp.logical.crud;

import org.apache.iotdb.db.qp.utils.GroupByLevelController;

public class SpecialClauseComponent {

  protected int rowLimit = 0;
  protected int rowOffset = 0;
  protected int seriesLimit = 0;
  protected int seriesOffset = 0;

  protected boolean ascending = true;
  // if true, we don't need the row whose any column is null
  protected boolean withoutAnyNull;
  // if true, we don't need the row whose all columns are null
  protected boolean withoutAllNull;

  protected GroupByLevelController groupByLevelController;
  protected int[] levels;

  protected boolean isAlignByDevice = false;
  protected boolean isAlignByTime = true;

  public SpecialClauseComponent() {}

  public int getRowLimit() {
    return rowLimit;
  }

  public void setRowLimit(int rowLimit) {
    this.rowLimit = rowLimit;
  }

  public int getRowOffset() {
    return rowOffset;
  }

  public void setRowOffset(int rowOffset) {
    this.rowOffset = rowOffset;
  }

  public boolean hasLimit() {
    return rowLimit > 0;
  }

  public int getSeriesLimit() {
    return seriesLimit;
  }

  public void setSeriesLimit(int seriesLimit) {
    this.seriesLimit = seriesLimit;
  }

  public int getSeriesOffset() {
    return seriesOffset;
  }

  public void setSeriesOffset(int seriesOffset) {
    this.seriesOffset = seriesOffset;
  }

  public boolean hasSlimit() {
    return seriesLimit > 0;
  }

  public boolean hasSoffset() {
    return seriesOffset > 0;
  }

  public boolean isAscending() {
    return ascending;
  }

  public void setAscending(boolean ascending) {
    this.ascending = ascending;
  }

  public boolean isWithoutAnyNull() {
    return withoutAnyNull;
  }

  public void setWithoutAnyNull(boolean withoutAnyNull) {
    this.withoutAnyNull = withoutAnyNull;
  }

  public boolean isWithoutAllNull() {
    return withoutAllNull;
  }

  public void setWithoutAllNull(boolean withoutAllNull) {
    this.withoutAllNull = withoutAllNull;
  }

  public int[] getLevels() {
    return levels;
  }

  public void setLevels(int[] levels) {
    this.levels = levels;
  }

  public void setGroupByLevelController(GroupByLevelController groupByLevelController) {
    this.groupByLevelController = groupByLevelController;
  }

  public boolean isAlignByDevice() {
    return isAlignByDevice;
  }

  public void setAlignByDevice(boolean isAlignByDevice) {
    this.isAlignByDevice = isAlignByDevice;
  }

  public boolean isAlignByTime() {
    return isAlignByTime;
  }

  public void setAlignByTime(boolean isAlignByTime) {
    this.isAlignByTime = isAlignByTime;
  }
}
