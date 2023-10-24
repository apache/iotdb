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

package org.apache.iotdb.db.pipe.metric;

import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class PipeEventCounter {

  private final AtomicInteger tabletInsertionEventCount = new AtomicInteger(0);
  private final AtomicInteger tsFileInsertionEventCount = new AtomicInteger(0);
  private final AtomicInteger pipeHeartbeatEventCount = new AtomicInteger(0);

  public Integer getTsFileInsertionEventCount() {
    return tsFileInsertionEventCount.get();
  }

  public Integer getTabletInsertionEventCount() {
    return tabletInsertionEventCount.get();
  }

  public Integer getPipeHeartbeatEventCount() {
    return pipeHeartbeatEventCount.get();
  }

  public void increaseEventCount(Event event) {
    if (Objects.isNull(event)) {
      return;
    }
    if (event instanceof PipeHeartbeatEvent) {
      pipeHeartbeatEventCount.incrementAndGet();
    } else if (event instanceof TabletInsertionEvent) {
      tabletInsertionEventCount.incrementAndGet();
    } else if (event instanceof TsFileInsertionEvent) {
      tsFileInsertionEventCount.incrementAndGet();
    }
  }

  public void decreaseEventCount(Event event) {
    if (Objects.isNull(event)) {
      return;
    }
    if (event instanceof PipeHeartbeatEvent) {
      pipeHeartbeatEventCount.decrementAndGet();
    } else if (event instanceof TabletInsertionEvent) {
      tabletInsertionEventCount.decrementAndGet();
    } else if (event instanceof TsFileInsertionEvent) {
      tsFileInsertionEventCount.decrementAndGet();
    }
  }

  public void reset() {
    tabletInsertionEventCount.set(0);
    tsFileInsertionEventCount.set(0);
    pipeHeartbeatEventCount.set(0);
  }
}
