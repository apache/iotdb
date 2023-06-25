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

package org.apache.iotdb.db.pipe.extractor.realtime;

import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;

public class PipeRealtimeDataRegionFakeExtractor extends PipeRealtimeDataRegionExtractor {

  @Override
  public void validate(PipeParameterValidator validator) {
    // do nothing
  }

  @Override
  public void customize(
      PipeParameters parameters, PipeExtractorRuntimeConfiguration configuration) {
    // do nothing
  }

  @Override
  public void start() {
    // do nothing
  }

  @Override
  public Event supply() {
    return null;
  }

  @Override
  public void extract(PipeRealtimeEvent event) {
    // do nothing
  }

  @Override
  public boolean isNeedListenToTsFile() {
    return false;
  }

  @Override
  public boolean isNeedListenToInsertNode() {
    return false;
  }

  @Override
  public void close() {
    // do nothing
  }

  @Override
  public String toString() {
    return "PipeRealtimeDataRegionFakeExtractor{}";
  }
}
