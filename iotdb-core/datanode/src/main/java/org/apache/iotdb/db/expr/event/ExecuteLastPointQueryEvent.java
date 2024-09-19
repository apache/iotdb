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

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import org.apache.iotdb.db.expr.conf.SimulationConfig;
import org.apache.iotdb.db.expr.entity.SimDeletion;
import org.apache.iotdb.db.expr.entity.SimTsFile;
import org.apache.iotdb.db.expr.simulator.SimulationContext;
import org.apache.tsfile.read.common.TimeRange;

public class ExecuteLastPointQueryEvent extends ExecuteRangeQueryEvent {

  public ExecuteLastPointQueryEvent(
      SimulationConfig config, TimeRange timeRange, Supplier<Long> intervalGenerator) {
    super(config, timeRange, 0, intervalGenerator);
  }

  protected TimeRange nextTimeRange(SimulationContext context) {
    long currentTimestamp = context.getSimulator().currentTimestamp;
    long lastTsFileVersion = currentTimestamp / context.getConfig().generateTsFileInterval;
    long lastTsFileTime = lastTsFileVersion * context.getConfig().tsfileRange;
    return new TimeRange(lastTsFileTime,lastTsFileTime + 1);
  }

  @Override
  public List<Event> nextEvents(SimulationContext context) {
    ExecuteLastPointQueryEvent event =
        new ExecuteLastPointQueryEvent(config, nextTimeRange(context), intervalGenerator);
    event.generateTimestamp =
        context.getSimulator().getCurrentTimestamp() + intervalGenerator.get();
    return Collections.singletonList(event);
  }
}
