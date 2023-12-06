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

package org.apache.iotdb.consensus.ratis.metrics;

import org.apache.iotdb.metrics.type.Timer;

import org.apache.ratis.metrics.Timekeeper;

import java.util.concurrent.TimeUnit;

/** TimerProxy will route Ratis' internal timer metrics to our IoTDB {@link Timer} */
public class TimerProxy implements Timekeeper {

  private static class TimerContext implements Timekeeper.Context {

    private final Timer reporter;
    private final long startTime;

    TimerContext(Timer reporter) {
      this.reporter = reporter;
      this.startTime = System.nanoTime();
    }

    @Override
    public long stop() {
      final long elapsed = System.nanoTime() - startTime;
      reporter.update(elapsed, TimeUnit.NANOSECONDS);
      return elapsed;
    }
  }

  private final Timer timer;

  TimerProxy(Timer timer) {
    this.timer = timer;
  }

  @Override
  public Context time() {
    return new TimerContext(timer);
  }
}
