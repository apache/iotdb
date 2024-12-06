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

package org.apache.iotdb.db.pipe.processor.downsampling;

public abstract class DownSamplingFilter {

  protected boolean isFilteredByArrivalTime = true;

  protected long lastPointArrivalTime;

  protected long lastPointEventTime;

  public DownSamplingFilter(
      final long arrivalTime, final long eventTime, final boolean isFilteredByArrivalTime) {
    this.lastPointArrivalTime = arrivalTime;
    this.lastPointEventTime = eventTime;
    this.isFilteredByArrivalTime = isFilteredByArrivalTime;
  }

  public DownSamplingFilter(final long arrivalTime, final long eventTime) {
    this.lastPointArrivalTime = arrivalTime;
    this.lastPointEventTime = eventTime;
  }

  public void reset(final long arrivalTime, final long eventTime) {
    this.lastPointArrivalTime = arrivalTime;
    this.lastPointEventTime = eventTime;
  }

  public long getLastPointArrivalTime() {
    return lastPointArrivalTime;
  }

  public long getLastPointEventTime() {
    return lastPointEventTime;
  }

  public boolean isFilteredByArrivalTime() {
    return isFilteredByArrivalTime;
  }
}
