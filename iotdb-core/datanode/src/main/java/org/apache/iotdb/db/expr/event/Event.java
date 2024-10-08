/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.expr.event;

import org.apache.iotdb.db.expr.conf.SimulationConfig;
import org.apache.iotdb.db.expr.simulator.SimulationContext;

import java.util.List;

public abstract class Event implements Comparable<Event> {
  public long generateTimestamp;
  public long executionTimestamp;
  public long executionStep;

  protected SimulationConfig config;
  protected long timeConsumption = -1;

  public Event(SimulationConfig config) {
    this.config = config;
  }

  public abstract List<Event> nextEvents(SimulationContext context);

  @Override
  public int compareTo(Event o) {
    return Long.compare(this.generateTimestamp, o.generateTimestamp);
  }

  public long getTimeConsumption() {
    if (timeConsumption == -1) {
      timeConsumption = calTimeConsumption();
    }
    return timeConsumption;
  }

  protected abstract long calTimeConsumption();
}
