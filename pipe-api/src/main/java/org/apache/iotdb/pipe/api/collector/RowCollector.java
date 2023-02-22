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

import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.event.insertion.TabletInsertionEvent;

import java.io.IOException;
import java.util.function.BiConsumer;

/**
 * Used to collect rows generated by {@link TabletInsertionEvent#processRowByRow(BiConsumer)},{@link
 * TabletInsertionEvent#processByIterator(BiConsumer)} or {@link
 * TabletInsertionEvent#processTablet(BiConsumer)}.
 */
public interface RowCollector {

  /**
   * Collects a row.
   *
   * @param row Row to be collected
   * @throws IOException if any I/O errors occur
   * @see Row
   */
  void collectRow(Row row) throws IOException;
}
