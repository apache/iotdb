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

package org.apache.iotdb.db.pipe.core.collector.realtime.cache;

import org.apache.iotdb.db.pipe.core.collector.realtime.PipeRealtimeCollector;
import org.apache.iotdb.db.pipe.core.collector.realtime.matcher.MapMatcher;
import org.apache.iotdb.db.pipe.core.collector.realtime.matcher.PipePatternMatcher;
import org.apache.iotdb.db.pipe.core.event.PipeCollectEvent;
import org.apache.iotdb.db.pipe.core.queue.DisruptorQueue;

import com.lmax.disruptor.dsl.ProducerType;

public class DataRegionChangeDataCache {
  private final PipePatternMatcher matcher;
  private final DisruptorQueue<PipeCollectEvent> disruptor;

  public DataRegionChangeDataCache() {
    this.matcher = new MapMatcher();

    this.disruptor =
        new DisruptorQueue.Builder<PipeCollectEvent>()
            .setProducerType(ProducerType.SINGLE)
            .addEventHandler(this::dispatchToCollectors)
            .build();
  }

  private void dispatchToCollectors(PipeCollectEvent event, long sequence, boolean endOfBatch) {
    matcher.match(event.getSchemaInfo()).forEach(collector -> collector.collectEvent(event));
    event.clearSchemaInfo();
  }

  public void publishCollectorEvent(PipeCollectEvent event) {
    disruptor.publish(event);
  }

  public void register(PipeRealtimeCollector collector) {
    matcher.register(collector);
  }

  public void deregister(PipeRealtimeCollector collector) {
    matcher.deregister(collector);
  }

  public void clear() {
    matcher.clear();
    disruptor.clear();
  }
}
