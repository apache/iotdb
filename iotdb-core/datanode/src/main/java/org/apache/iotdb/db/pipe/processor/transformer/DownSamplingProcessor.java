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

package org.apache.iotdb.db.pipe.processor.transformer;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.db.pipe.config.plugin.env.PipeTaskProcessorRuntimeEnvironment;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.processor.utils.PartialPathLastTimeCache;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.db.pipe.config.constant.PipeProcessorConstant.PROCESSOR_DOWN_SAMPLING_INTERVAL_SECONDS_DEFAULT_VALUE;
import static org.apache.iotdb.db.pipe.config.constant.PipeProcessorConstant.PROCESSOR_DOWN_SAMPLING_INTERVAL_SECONDS_KEY;

public class DownSamplingProcessor implements PipeProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(DownSamplingProcessor.class);

  private long intervalSeconds;
  private String dataBaseName;
  private PartialPathLastTimeCache partialPathLastTimeCache;

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    // Do nothing
  }

  // TODO: Implement multiple down sampling methods, like "average"
  @Override
  public void customize(PipeParameters parameters, PipeProcessorRuntimeConfiguration configuration)
      throws Exception {
    intervalSeconds =
        parameters.getLongOrDefault(
            PROCESSOR_DOWN_SAMPLING_INTERVAL_SECONDS_KEY,
            PROCESSOR_DOWN_SAMPLING_INTERVAL_SECONDS_DEFAULT_VALUE);
    LOGGER.info("DownSamplingProcessor intervalSeconds: {}", intervalSeconds);

    dataBaseName =
        StorageEngine.getInstance()
            .getDataRegion(
                new DataRegionId(
                    ((PipeTaskProcessorRuntimeEnvironment) configuration.getRuntimeEnvironment())
                        .getRegionId()))
            .getDatabaseName();

    partialPathLastTimeCache = new PartialPathLastTimeCache();
  }

  @Override
  public void process(TabletInsertionEvent tabletInsertionEvent, EventCollector eventCollector)
      throws Exception {
    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      LOGGER.warn(
          "DownSamplingProcessor only support PipeInsertNodeTabletInsertionEvent and PipeRawTabletInsertionEvent. "
              + "Current event: {}.",
          tabletInsertionEvent);
      return;
    }

    boolean isAligned =
        tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent
            ? ((PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent).isAligned()
            : ((PipeRawTabletInsertionEvent) tabletInsertionEvent).isAligned();

    AtomicReference<Exception> exception = new AtomicReference<>();
    tabletInsertionEvent
        .processRowByRow(
            (row, rowCollector) -> {
              String deviceSuffix =
                  row.getDeviceId().replaceFirst(dataBaseName + TsFileConstant.PATH_SEPARATOR, "");
              if (isAligned) {
                Long lastSendTime = partialPathLastTimeCache.getPartialPathLastTime(deviceSuffix);
                if (lastSendTime != null) {
                  if (row.getTime() - lastSendTime
                      >= TimestampPrecisionUtils.convertToCurrPrecision(
                          intervalSeconds, TimeUnit.SECONDS)) {
                    partialPathLastTimeCache.setPartialPathLastTime(deviceSuffix, row.getTime());
                    try {
                      rowCollector.collectRow(row);
                    } catch (Exception e) {
                      exception.set(e);
                    }
                  }
                } else {
                  partialPathLastTimeCache.setPartialPathLastTime(deviceSuffix, row.getTime());
                  try {
                    rowCollector.collectRow(row);
                  } catch (Exception e) {
                    exception.set(e);
                  }
                }
              } else {
                boolean hasNonNull = false;
                for (int index = 0; index < row.size(); ++index) {
                  if (row.isNull(index)) {
                    continue;
                  }
                  String timeSeriesSuffix =
                      deviceSuffix + TsFileConstant.PATH_SEPARATOR + row.getColumnName(index);
                  Long lastSendTime =
                      partialPathLastTimeCache.getPartialPathLastTime(timeSeriesSuffix);
                  if (lastSendTime != null) {
                    if (row.getTime() - lastSendTime
                        >= TimestampPrecisionUtils.convertToCurrPrecision(
                            intervalSeconds, TimeUnit.SECONDS)) {
                      hasNonNull = true;
                      partialPathLastTimeCache.setPartialPathLastTime(
                          timeSeriesSuffix, row.getTime());
                    } else {
                      // row.setNull(index);
                    }
                  } else {
                    hasNonNull = true;
                    partialPathLastTimeCache.setPartialPathLastTime(
                        timeSeriesSuffix, row.getTime());
                  }
                }
                if (hasNonNull) {
                  try {
                    rowCollector.collectRow(row);
                  } catch (Exception e) {
                    exception.set(e);
                  }
                }
              }
            })
        .forEach(
            event -> {
              try {
                eventCollector.collect(event);
              } catch (Exception e) {
                exception.set(e);
              }
            });
    if (exception.get() != null) {
      throw exception.get();
    }
  }

  @Override
  public void process(Event event, EventCollector eventCollector) throws Exception {
    eventCollector.collect(event);
  }

  @Override
  public void close() throws Exception {
    if (partialPathLastTimeCache != null) {
      partialPathLastTimeCache.close();
    }
  }
}
