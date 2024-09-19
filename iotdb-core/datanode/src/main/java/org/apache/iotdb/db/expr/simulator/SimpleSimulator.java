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

package org.apache.iotdb.db.expr.simulator;

import java.util.List;
import org.apache.iotdb.db.expr.conf.SimulationConfig;
import org.apache.iotdb.db.expr.entity.SimModFileManager;
import org.apache.iotdb.db.expr.entity.SimTsFileManager;
import org.apache.iotdb.db.expr.event.Event;
import org.apache.iotdb.db.expr.executor.SimpleExecutor;

import java.util.PriorityQueue;

public class SimpleSimulator {
  public long currentStep;
  public long currentTimestamp;
  private final PriorityQueue<Event> eventQueue = new PriorityQueue<>();
  private final SimpleExecutor eventExecutor = new SimpleExecutor();
  private final SimpleContext simulationContext = new SimpleContext();
  private final SimulationStatistics statistics = new SimulationStatistics();
  private final SimulationConfig simulationConfig;

  public long maxStep = Long.MAX_VALUE;
  public long maxTimestamp = Long.MAX_VALUE;

  public SimpleSimulator(SimulationConfig config) {
    this.simulationConfig = config;
  }

  public void addEvent(Event event) {
    eventQueue.add(event);
  }

  public void addEvents(List<Event> events) {
    eventQueue.addAll(events);
  }

  public void start() {
    doSimulate();
  }

  public void forwardTimeTo(long nextTimestamp) {
    currentTimestamp = nextTimestamp;
  }

  public void forwardTime(long delta) {
    currentTimestamp += delta;
  }

  private void doSimulate() {
    while (!eventQueue.isEmpty() && currentTimestamp < maxTimestamp && currentStep < maxStep) {
      Event event = eventQueue.poll();
      if (event.generateTimestamp > currentTimestamp) {
        forwardTimeTo(event.generateTimestamp);
      }
      eventExecutor.execute(event, simulationContext);
      addEvents(event.nextEvents(simulationContext));
      currentStep++;
    }
  }

  public long getCurrentStep() {
    return currentStep;
  }

  public long getCurrentTimestamp() {
    return currentTimestamp;
  }

  public SimulationStatistics getStatistics() {
    return statistics;
  }

  public SimpleContext getSimulationContext() {
    return simulationContext;
  }

  @Override
  public String toString() {
    return "SimpleSimulator{" +
        "currentTimestamp=" + currentTimestamp +
        "\n, currentStep=" + currentStep +
        "\n, eventQueue=" + eventQueue +
        "\n, maxStep=" + maxStep +
        "\n, maxTimestamp=" + maxTimestamp +
        '}';
  }

  public class SimpleContext implements SimulationContext {

    public SimTsFileManager tsFileManager = new SimTsFileManager();
    public SimModFileManager modFileManager = new SimModFileManager();

    @Override
    public SimpleSimulator getSimulator() {
      return SimpleSimulator.this;
    }

    @Override
    public SimulationStatistics getStatistics() {
      return statistics;
    }

    @Override
    public SimulationConfig getConfig() {
      return simulationConfig;
    }
  }
}
