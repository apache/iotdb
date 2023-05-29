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

package org.apache.iotdb.pipe.api.collector;

import org.apache.iotdb.pipe.api.event.Event;

import java.io.IOException;

/** Used to collect events in pipe engine. */
public interface EventCollector {

  /**
   * Collects a Event in pipe engine.
   *
   * @param event Event to be collected
   * @throws IOException if any I/O errors occur
   * @see Event
   * @see org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent
   * @see org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent
   */
  void collect(Event event) throws IOException;
}
