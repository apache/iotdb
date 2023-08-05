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

package org.apache.iotdb.db.pipe.extractor.realtime.assigner;

import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.extractor.realtime.PipeRealtimeDataRegionExtractor;
import org.apache.iotdb.db.pipe.extractor.realtime.matcher.CachedSchemaPatternMatcher;
import org.apache.iotdb.db.pipe.extractor.realtime.matcher.PipeDataRegionMatcher;

public class PipeDataRegionAssigner {

  /** The matcher is used to match the event with the extractor based on the pattern. */
  private final PipeDataRegionMatcher matcher;

  /** The disruptor is used to assign the event to the extractor. */
  private final DisruptorQueue disruptor;

  public PipeDataRegionAssigner() {
    this.matcher = new CachedSchemaPatternMatcher();
    this.disruptor = new DisruptorQueue(this::assignToExtractor);
  }

  public void publishToAssign(PipeRealtimeEvent event) {
    event.increaseReferenceCount(PipeDataRegionAssigner.class.getName());
    disruptor.publish(event);
  }

  public void assignToExtractor(PipeRealtimeEvent event, long sequence, boolean endOfBatch) {
    matcher
        .match(event)
        .forEach(
            extractor -> {
              final PipeRealtimeEvent copiedEvent =
                  event.shallowCopySelfAndBindPipeTaskMetaForProgressReport(
                      extractor.getPipeTaskMeta(), extractor.getPattern());
              copiedEvent.increaseReferenceCount(PipeDataRegionAssigner.class.getName());
              extractor.extract(copiedEvent);
            });
    event.gcSchemaInfo();
    event.decreaseReferenceCount(PipeDataRegionAssigner.class.getName());
  }

  public void startAssignTo(PipeRealtimeDataRegionExtractor extractor) {
    matcher.register(extractor);
  }

  public void stopAssignTo(PipeRealtimeDataRegionExtractor extractor) {
    matcher.deregister(extractor);
  }

  public boolean notMoreExtractorNeededToBeAssigned() {
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
