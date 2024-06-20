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

package org.apache.iotdb;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

public class CountPointProcessor implements PipeProcessor {
  private static final String AGGREGATE_SERIES_KEY = "aggregate-series";
  private static final AtomicLong writePointCount = new AtomicLong(0);

  private PartialPath aggregateSeries;

  @Override
  public void validate(final PipeParameterValidator validator) {
    validator.validateRequiredAttribute(AGGREGATE_SERIES_KEY);
  }

  @Override
  public void customize(
      final PipeParameters parameters, final PipeProcessorRuntimeConfiguration configuration)
      throws Exception {
    this.aggregateSeries = new PartialPath(parameters.getString(AGGREGATE_SERIES_KEY));
  }

  @Override
  public void process(
      final TabletInsertionEvent tabletInsertionEvent, final EventCollector eventCollector) {
    tabletInsertionEvent.processTablet(
        (tablet, rowCollector) -> writePointCount.addAndGet(tablet.rowSize));
  }

  @Override
  public void process(final Event event, final EventCollector eventCollector) throws Exception {
    if (event instanceof PipeHeartbeatEvent) {
      final Tablet tablet =
          new Tablet(
              aggregateSeries.getIDeviceID().toString(),
              Collections.singletonList(
                  new MeasurementSchema(aggregateSeries.getMeasurement(), TSDataType.INT64)),
              1);
      tablet.rowSize = 1;
      tablet.addTimestamp(0, System.currentTimeMillis());
      tablet.addValue(aggregateSeries.getMeasurement(), 0, writePointCount.get());
      eventCollector.collect(
          new PipeRawTabletInsertionEvent(tablet, false, null, 0, null, null, false));
    }
  }

  @Override
  public void close() {
    // Do nothing
  }
}
