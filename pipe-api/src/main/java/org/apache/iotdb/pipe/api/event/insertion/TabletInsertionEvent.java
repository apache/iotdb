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

import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.tsfile.write.record.Tablet;

import java.util.Iterator;
import java.util.function.BiConsumer;

/** TabletInsertionEvent is used to define the event of data insertion. */
public interface TabletInsertionEvent extends Event {

  /**
   * The consumer processes the data row by row and collects the results by RowCollector.
   *
   * @return TabletInsertionEvent a new TabletInsertionEvent contains the results collected by the
   *     RowCollector
   */
  TabletInsertionEvent processRowByRow(BiConsumer<Row, RowCollector> consumer);

  /**
   * The consumer processes the data by the Iterator and collects the results by RowCollector.
   *
   * @return TabletInsertionEvent a new TabletInsertionEvent contains the results collected by the
   *     RowCollector
   */
  TabletInsertionEvent processByIterator(BiConsumer<Iterator<Row>, RowCollector> consumer);

  /**
   * The consumer processes the Tablet directly and collects the results by RowCollector.
   *
   * @return TabletInsertionEvent a new TabletInsertionEvent contains the results collected by the
   *     RowCollector
   */
  TabletInsertionEvent processTablet(BiConsumer<Tablet, RowCollector> consumer);
}
