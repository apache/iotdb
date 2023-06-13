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

package org.apache.iotdb.db.pipe.event.common.tablet;

import org.apache.iotdb.db.pipe.config.PipeCollectorConstant;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.record.Tablet;

import java.util.Objects;
import java.util.function.BiConsumer;

public class PipeRawTabletInsertionEvent implements TabletInsertionEvent {

  private final Tablet tablet;
  private final boolean isAligned;
  private final String pattern;

  private TabletInsertionDataContainer dataContainer;

  public PipeRawTabletInsertionEvent(Tablet tablet) {
    this(Objects.requireNonNull(tablet), false, null);
  }

  public PipeRawTabletInsertionEvent(Tablet tablet, String pattern) {
    this(tablet, false, pattern);
  }

  public PipeRawTabletInsertionEvent(Tablet tablet, boolean isAligned) {
    this(tablet, isAligned, null);
  }

  public PipeRawTabletInsertionEvent(Tablet tablet, boolean isAligned, String pattern) {
    this.tablet = Objects.requireNonNull(tablet);
    this.isAligned = isAligned;
    this.pattern = pattern;
  }

  public String getPattern() {
    return pattern == null ? PipeCollectorConstant.COLLECTOR_PATTERN_DEFAULT_VALUE : pattern;
  }

  /////////////////////////// TabletInsertionEvent ///////////////////////////

  @Override
  public Iterable<TabletInsertionEvent> processRowByRow(BiConsumer<Row, RowCollector> consumer) {
    if (dataContainer == null) {
      dataContainer = new TabletInsertionDataContainer(tablet, isAligned, getPattern());
    }
    return dataContainer.processRowByRow(consumer);
  }

  @Override
  public Iterable<TabletInsertionEvent> processTablet(
      BiConsumer<Pair<Tablet, Boolean>, RowCollector> consumer) {
    if (dataContainer == null) {
      dataContainer = new TabletInsertionDataContainer(tablet, isAligned, getPattern());
    }
    return dataContainer.processTablet(consumer);
  }

  public Pair<Tablet, Boolean> convertToTabletWithIsAligned() {
    final String pattern = getPattern();

    // if pattern is "root", we don't need to convert, just return the original tablet
    if (pattern.equals(PipeCollectorConstant.COLLECTOR_PATTERN_DEFAULT_VALUE)) {
      return new Pair<>(tablet, isAligned);
    }

    // if pattern is not "root", we need to convert the tablet
    if (dataContainer == null) {
      dataContainer = new TabletInsertionDataContainer(tablet, isAligned, pattern);
    }
    return dataContainer.convertToTablet();
  }
}
