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

import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.tsfile.write.record.Tablet;

/**
 * {@link PipeTabletInsertionEvent} is used to define the event of tablet data insertion, with some
 * additional common interfaces used in IoTDB synchronization.
 */
public interface PipeTabletInsertionEvent extends TabletInsertionEvent {

  /**
   * Convert the data contained to tablet.
   *
   * @return the result tablet
   */
  Tablet convertToTablet();

  /**
   * Parse the event to resolve its pattern. The parsing process is done in
   * PipeRawTabletInsertionEvent.
   *
   * @return the converted PipeRawTabletInsertionEvent
   */
  TabletInsertionEvent parseEventWithPattern();

  /**
   * Return whether the tablet is aligned.
   *
   * @return true if the tablet is aligned
   */
  boolean isAligned();
}
