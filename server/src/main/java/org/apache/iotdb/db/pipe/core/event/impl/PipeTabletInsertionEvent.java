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

package org.apache.iotdb.db.pipe.core.event.impl;

import org.apache.iotdb.db.pipe.config.constant.PipeCollectorConstant;
import org.apache.iotdb.db.pipe.core.event.view.datastructure.TabletInsertionDataContainer;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.tsfile.write.record.Tablet;

import java.util.Objects;
import java.util.function.BiConsumer;

public class PipeTabletInsertionEvent implements TabletInsertionEvent {

  private final Tablet tablet;
  private final String pattern;

  private TabletInsertionDataContainer dataContainer;

  public PipeTabletInsertionEvent(Tablet tablet) {
    this(Objects.requireNonNull(tablet), null);
  }

  public PipeTabletInsertionEvent(Tablet tablet, String pattern) {
    this.tablet = Objects.requireNonNull(tablet);
    this.pattern = pattern;
  }

  public String getPattern() {
    return pattern == null ? PipeCollectorConstant.COLLECTOR_PATTERN_DEFAULT_VALUE : pattern;
  }

  /////////////////////////// TabletInsertionEvent ///////////////////////////

  @Override
  public TabletInsertionEvent processRowByRow(BiConsumer<Row, RowCollector> consumer) {
    if (dataContainer == null) {
      dataContainer = new TabletInsertionDataContainer(tablet, getPattern());
    }
    return dataContainer.processRowByRow(consumer);
  }

  @Override
  public TabletInsertionEvent processTablet(BiConsumer<Tablet, RowCollector> consumer) {
    if (dataContainer == null) {
      dataContainer = new TabletInsertionDataContainer(tablet, getPattern());
    }
    return dataContainer.processTablet(consumer);
  }

  public Tablet convertToTablet() {
    if (dataContainer == null) {
      dataContainer = new TabletInsertionDataContainer(tablet, getPattern());
    }
    return dataContainer.convertToTablet();
  }
}
