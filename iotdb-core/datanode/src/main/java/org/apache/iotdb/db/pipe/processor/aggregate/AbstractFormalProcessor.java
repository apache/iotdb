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

package org.apache.iotdb.db.pipe.processor.aggregate;

import org.apache.iotdb.pipe.api.PipePlugin;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

/**
 * {@link AbstractFormalProcessor} is a formal {@link PipeProcessor} that does not support data
 * processing. It is mainly used as a configurable plugin for other {@link PipePlugin}s that can be
 * dynamically loaded into IoTDB.
 */
public abstract class AbstractFormalProcessor implements PipeProcessor {
  @Override
  public final void process(
      final TabletInsertionEvent tabletInsertionEvent, final EventCollector eventCollector)
      throws Exception {
    throw new UnsupportedOperationException(
        "The abstract formal processor does not support process events");
  }

  @Override
  public final void process(
      final TsFileInsertionEvent tsFileInsertionEvent, final EventCollector eventCollector)
      throws Exception {
    throw new UnsupportedOperationException(
        "The abstract formal processor does not support process events");
  }

  @Override
  public final void process(final Event event, final EventCollector eventCollector)
      throws Exception {
    throw new UnsupportedOperationException(
        "The abstract formal processor does not support process events");
  }
}
