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
import org.apache.iotdb.db.expr.entity.SimDeletion;
import org.apache.iotdb.db.expr.entity.SimTsFile;
import org.apache.iotdb.db.expr.simulator.SimulationContext;

import org.apache.tsfile.read.common.TimeRange;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class ExecuteRangeQueryEvent extends Event {

  public TimeRange timeRange;
  public long step;
  protected final Supplier<Long> intervalGenerator;

  public List<SimTsFile> queriedTsFiles;
  public List<List<SimDeletion>> queriedDeletions;

  public double readTsFileTimeSum = 0;
  public double readDeletionTimeSum = 0;
  public double readDeletionSeekTimeSum = 0;
  public double readDeletionTransTimeSum = 0;

  public ExecuteRangeQueryEvent(
      SimulationConfig config, TimeRange timeRange, long step, Supplier<Long> intervalGenerator) {
    super(config);
    this.timeRange = timeRange;
    this.step = step;
    this.intervalGenerator = intervalGenerator;
  }

  protected TimeRange nextTimeRange(SimulationContext context) {
    return new TimeRange(timeRange.getMin() + step, timeRange.getMax() + step);
  }

  @Override
  public List<Event> nextEvents(SimulationContext context) {
    ExecuteRangeQueryEvent event =
        new ExecuteRangeQueryEvent(config, nextTimeRange(context), step, intervalGenerator);
    event.generateTimestamp =
        context.getSimulator().getCurrentTimestamp() + intervalGenerator.get();
    return Collections.singletonList(event);
  }

  protected double readTsFileConsumption(SimTsFile tsFile) {
    return 1.0
            * (tsFile.timeRange.getMax() - tsFile.timeRange.getMin())
            * config.timeRangeToBytesFactor
            / config.IoBandwidthBytesPerTimestamp
        + config.IoSeekTimestamp;
  }

  protected double readDeletionsConsumption(List<SimDeletion> deletions) {
    double transTime =
        1.0 * deletions.size() * config.deletionSizeInByte / config.IoBandwidthBytesPerTimestamp;
    readDeletionTransTimeSum += transTime;
    readDeletionSeekTimeSum += config.IoSeekTimestamp;
    return transTime + config.IoSeekTimestamp;
  }

  @Override
  public long calTimeConsumption() {
    for (int i = 0; i < queriedTsFiles.size(); i++) {
      SimTsFile simTsFile = queriedTsFiles.get(i);
      readTsFileTimeSum += readTsFileConsumption(simTsFile);
      if (simTsFile.hasModFile()) {
        readDeletionTimeSum += readDeletionsConsumption(queriedDeletions.get(i));
      }
    }

    return Math.round(readTsFileTimeSum + readDeletionTimeSum);
  }
}
