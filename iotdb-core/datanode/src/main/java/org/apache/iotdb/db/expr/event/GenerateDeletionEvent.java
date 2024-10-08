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

package org.apache.iotdb.db.expr.event;

import org.apache.iotdb.db.expr.conf.SimulationConfig;
import org.apache.iotdb.db.expr.entity.SimDeletion;
import org.apache.iotdb.db.expr.entity.SimModFile;
import org.apache.iotdb.db.expr.simulator.SimulationContext;

import org.apache.tsfile.read.common.TimeRange;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class GenerateDeletionEvent extends Event {

  public SimDeletion currentDeletion;
  public long step;
  private final Supplier<Long> intervalGenerator;

  public Collection<SimModFile> involvedModFiles;

  public GenerateDeletionEvent(
      SimulationConfig config,
      SimDeletion currentDeletion,
      long step,
      Supplier<Long> intervalGenerator) {
    super(config);

    this.currentDeletion = currentDeletion;
    this.step = step;
    this.intervalGenerator = intervalGenerator;
  }

  public SimDeletion nextDeletion() {
    return new SimDeletion(
        new TimeRange(
            currentDeletion.timeRange.getMin() + step, currentDeletion.timeRange.getMax() + step));
  }

  @Override
  public List<Event> nextEvents(SimulationContext context) {
    GenerateDeletionEvent event =
        new GenerateDeletionEvent(config, nextDeletion(), step, intervalGenerator);
    event.generateTimestamp =
        context.getSimulator().getCurrentTimestamp() + intervalGenerator.get();
    return Collections.singletonList(event);
  }

  @Override
  public long calTimeConsumption() {
    double sum = 0.0;
    sum +=
        involvedModFiles.size() * config.IoSeekTimestamp
            + 1.0
                * involvedModFiles.size()
                * config.deletionSizeInByte
                / config.IoBandwidthBytesPerTimestamp;
    return Math.round(sum);
  }
}
