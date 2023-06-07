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

package org.apache.iotdb.db.pipe.collector.realtime;

import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeCollectEvent;
import org.apache.iotdb.pipe.api.customizer.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.PipeParameters;
import org.apache.iotdb.pipe.api.customizer.collector.PipeCollectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.event.Event;

public class PipeRealtimeDataRegionFakeCollector extends PipeRealtimeDataRegionCollector {
  @Override
  public void validate(PipeParameterValidator validator) {}

  @Override
  public void customize(
      PipeParameters parameters, PipeCollectorRuntimeConfiguration configuration) {}

  @Override
  public void start() {}

  @Override
  public Event supply() {
    return null;
  }

  @Override
  public void collect(PipeRealtimeCollectEvent event) {}

  @Override
  public boolean isNeedListenToTsFile() {
    return false;
  }

  @Override
  public boolean isNeedListenToInsertNode() {
    return false;
  }

  @Override
  public void close() {}

  @Override
  public String toString() {
    return "PipeRealtimeDataRegionFakeCollector{}";
  }
}
