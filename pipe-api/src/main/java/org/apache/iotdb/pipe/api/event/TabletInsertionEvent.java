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

package org.apache.iotdb.pipe.api.event;

import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.tsfile.write.record.Tablet;

import java.util.Iterator;
import java.util.function.BiConsumer;

/**
 * TabletInsertionEvent is used to define the event of writing data. Event data stores in memory,
 * which is not compressed or encoded, and can be processed directly for computation.
 */
public abstract class TabletInsertionEvent implements Event {

  /**
   * The consumer consumes the Tablet by rows, and collects the data by RowCollector.
   *
   * @return TabletInsertionEvent
   */
  public abstract TabletInsertionEvent processRowByRow(BiConsumer<Row, RowCollector> consumer);

  /**
   * The method provides Iterator to access data, and collects the data by RowCollector..
   *
   * @return TabletInsertionEvent
   */
  public abstract TabletInsertionEvent processByIterator(
      BiConsumer<Iterator<Row>, RowCollector> consumer);

  /**
   * The consumer consumes the Tablet directly, and collects the data by RowCollector.
   *
   * @return TabletInsertionEvent
   */
  public abstract TabletInsertionEvent process(BiConsumer<Tablet, RowCollector> consumer);
}
