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

package org.apache.iotdb.db.pipe.core.processor;

import org.apache.iotdb.db.pipe.config.PipeCollectorConstant;
import org.apache.iotdb.db.pipe.core.event.EnrichedEvent;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.customizer.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.PipeParameters;
import org.apache.iotdb.pipe.api.customizer.processor.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import java.io.IOException;

public class PipeDoNothingProcessor implements PipeProcessor {

  @Override
  public void validate(PipeParameterValidator validator) {
    // do nothing
  }

  @Override
  public void customize(
      PipeParameters parameters, PipeProcessorRuntimeConfiguration configuration) {
    // do nothing
  }

  @Override
  public void process(TabletInsertionEvent tabletInsertionEvent, EventCollector eventCollector)
      throws IOException {
    if (tabletInsertionEvent instanceof EnrichedEvent) {
      final EnrichedEvent enrichedEvent = (EnrichedEvent) tabletInsertionEvent;
      if (enrichedEvent
          .getPattern()
          .equals(PipeCollectorConstant.COLLECTOR_PATTERN_DEFAULT_VALUE)) {
        eventCollector.collect(tabletInsertionEvent);
      } else {
        tabletInsertionEvent
            .processRowByRow(
                (row, rowCollector) -> {
                  try {
                    rowCollector.collectRow(row);
                  } catch (IOException e) {
                    throw new PipeException("Failed to collect row", e);
                  }
                })
            .forEach(
                event -> {
                  try {
                    eventCollector.collect(event);
                  } catch (IOException e) {
                    throw new PipeException("Failed to collect event", e);
                  }
                });
      }
    } else {
      eventCollector.collect(tabletInsertionEvent);
    }
  }

  @Override
  public void process(TsFileInsertionEvent tsFileInsertionEvent, EventCollector eventCollector)
      throws IOException {
    if (tsFileInsertionEvent instanceof EnrichedEvent) {
      final EnrichedEvent enrichedEvent = (EnrichedEvent) tsFileInsertionEvent;
      if (enrichedEvent
          .getPattern()
          .equals(PipeCollectorConstant.COLLECTOR_PATTERN_DEFAULT_VALUE)) {
        eventCollector.collect(tsFileInsertionEvent);
      } else {
        for (final TabletInsertionEvent tabletInsertionEvent :
            tsFileInsertionEvent.toTabletInsertionEvents()) {
          eventCollector.collect(tabletInsertionEvent);
        }
      }
    } else {
      eventCollector.collect(tsFileInsertionEvent);
    }
  }

  @Override
  public void process(Event event, EventCollector eventCollector) throws IOException {
    eventCollector.collect(event);
  }

  @Override
  public void close() {
    // do nothing
  }
}
