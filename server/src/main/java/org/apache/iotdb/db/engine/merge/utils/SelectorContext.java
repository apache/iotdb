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
package org.apache.iotdb.db.engine.merge.utils;

public class SelectorContext {

  private long totalCost;
  private long startTime;
  private long timeConsumption;

  public SelectorContext() {
    this(0);
  }

  public SelectorContext(long totalCost) {
    this.totalCost = totalCost;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getTotalCost() {
    return totalCost;
  }

  public void incTotalCost(long cost) {
    this.totalCost += cost;
  }

  public void setTotalCost(long totalCost) {
    this.totalCost = totalCost;
  }

  public void clearTotalCost() {
    this.totalCost = 0;
  }

  public void clearTimeConsumption() {
    this.timeConsumption = 0;
  }

  public void updateTimeConsumption() {
    this.timeConsumption = System.currentTimeMillis() - startTime;
  }

  public long getTimeConsumption() {
    return timeConsumption;
  }
}
