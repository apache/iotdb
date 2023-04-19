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

package org.apache.iotdb.pipe.api.event.insertion;

import org.apache.iotdb.pipe.api.event.Event;

/**
 * TsFileInsertionEvent is used to define the event of writing TsFile. Event data stores in disks,
 * which is compressed and encoded, and requires IO cost for computational processing.
 */
public interface TsFileInsertionEvent extends Event {

  /**
   * The method is used to convert the TsFileInsertionEvent into several TabletInsertionEvents.
   *
   * @return the list of TsFileInsertionEvent
   */
  Iterable<TabletInsertionEvent> toTabletInsertionEvents();

  /**
   * The method is used to compact several TabletInsertionEvents into one TsFileInsertionEvent. The
   * underlying data in TabletInsertionEvents will be stored into a TsFile.
   *
   * @return TsFileInsertionEvent
   */
  TsFileInsertionEvent toTsFileInsertionEvent(Iterable<TabletInsertionEvent> iterable);
}
