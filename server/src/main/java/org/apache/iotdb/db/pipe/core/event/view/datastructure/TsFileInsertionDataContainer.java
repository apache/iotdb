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

package org.apache.iotdb.db.pipe.core.event.view.datastructure;

import org.apache.iotdb.db.pipe.core.event.impl.PipeTabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.write.record.Tablet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TsFileInsertionDataContainer {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileInsertionDataContainer.class);

  private final File tsFile;
  private final String pattern;

  private final Map<String, List<TimeseriesMetadata>> device2TimeseriesMetadataMap;

  public TsFileInsertionDataContainer(File tsFile, String pattern) {
    this.tsFile = tsFile;
    this.pattern = pattern;

    this.device2TimeseriesMetadataMap = collectDevice2TimeseriesMetadataMap();
  }

  private Map<String, List<TimeseriesMetadata>> collectDevice2TimeseriesMetadataMap() {
    final Map<String, List<TimeseriesMetadata>> result = new HashMap<>();

    try (TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getPath())) {
      // match pattern
      for (Map.Entry<String, List<TimeseriesMetadata>> entry :
          reader.getAllTimeseriesMetadata(true).entrySet()) {
        final String device = entry.getKey();

        // case 1: for example, pattern is root.a.b or pattern is null and device is root.a.b.c
        // in this case, all data can be matched without checking the measurements
        if (pattern == null || pattern.length() <= device.length() && device.startsWith(pattern)) {
          result.put(device, entry.getValue());
        }

        // case 2: for example, pattern is root.a.b.c and device is root.a.b
        // in this case, we need to check the full path
        else {
          final List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();

          for (TimeseriesMetadata timeseriesMetadata : entry.getValue()) {
            // TODO: test me!!!
            if (timeseriesMetadata.getTSDataType() == TSDataType.VECTOR) {
              timeseriesMetadataList.add(timeseriesMetadata);
              continue;
            }

            final String measurement = timeseriesMetadata.getMeasurementId();
            // low cost check comes first
            if (pattern.length() == measurement.length() + device.length() + 1
                // high cost check comes later
                && pattern.endsWith(TsFileConstant.PATH_SEPARATOR + measurement)) {
              timeseriesMetadataList.add(timeseriesMetadata);
            }
          }

          result.put(device, timeseriesMetadataList);
        }
      }
    } catch (IOException e) {
      LOGGER.error("Cannot read TsFile {}.", tsFile.getPath(), e);
    }

    return result;
  }

  public Iterable<TabletInsertionEvent> toTabletInsertionEvents() {
    return () ->
        new Iterator<TabletInsertionEvent>() {

          private final Iterator<Tablet> tabletIterator = constructTabletIterable().iterator();

          @Override
          public boolean hasNext() {
            return tabletIterator.hasNext();
          }

          @Override
          public TabletInsertionEvent next() {
            return new PipeTabletInsertionEvent(tabletIterator.next());
          }
        };
  }

  private Iterable<Tablet> constructTabletIterable() {
    return () ->
        new TsFileInsertionDataTabletIterator(tsFile.getPath(), device2TimeseriesMetadataMap);
  }
}
