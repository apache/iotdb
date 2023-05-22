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

package org.apache.iotdb.db.pipe.core.collector.realtime.assigner;

import org.apache.iotdb.db.pipe.core.collector.realtime.PipeRealtimeDataRegionCollector;
import org.apache.iotdb.db.pipe.core.collector.realtime.matcher.CachedSchemaPatternMatcher;
import org.apache.iotdb.db.pipe.core.collector.realtime.matcher.PipeDataRegionMatcher;
import org.apache.iotdb.db.pipe.core.event.realtime.PipeRealtimeCollectEvent;

import com.lmax.disruptor.dsl.ProducerType;

public class PipeDataRegionAssigner {

  /** The matcher is used to match the event with the collector based on the pattern. */
  private final PipeDataRegionMatcher matcher;

  /** The disruptor is used to assign the event to the collector. */
  private final DisruptorQueue<PipeRealtimeCollectEvent> disruptor;

  public PipeDataRegionAssigner() {
    this.matcher = new CachedSchemaPatternMatcher();
    this.disruptor =
        new DisruptorQueue.Builder<PipeRealtimeCollectEvent>()
            .setProducerType(ProducerType.SINGLE)
            .addEventHandler(this::assignToCollector)
            .build();
  }

  public void publishToAssign(PipeRealtimeCollectEvent event) {
    event.increaseReferenceCount(PipeDataRegionAssigner.class.getName());
    disruptor.publish(event);
  }

  public void assignToCollector(PipeRealtimeCollectEvent event, long sequence, boolean endOfBatch) {
    matcher
        .match(event)
        .forEach(
            collector -> {
              event.increaseReferenceCount(PipeDataRegionAssigner.class.getName());
              collector.collect(event);
            });
    event.gcSchemaInfo();
    event.decreaseReferenceCount(PipeDataRegionAssigner.class.getName());
  }

  public void startAssignTo(PipeRealtimeDataRegionCollector collector) {
    matcher.register(collector);
  }

  public void stopAssignTo(PipeRealtimeDataRegionCollector collector) {
    matcher.deregister(collector);
  }

  public boolean notMoreCollectorNeededToBeAssigned() {
    return matcher.getRegisterCount() == 0;
  }

  /**
   * Clear the matcher and disruptor. The method publishToAssign should be work after calling this
   * method.
   */
  public void gc() {
    matcher.clear();
    disruptor.clear();
  }
}
