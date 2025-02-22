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

package org.apache.iotdb.db.queryengine.execution.operator.process.function.partition;

import org.apache.iotdb.udf.api.relational.access.Record;

import java.util.ArrayList;
import java.util.List;

/** Used to manage the slices of the partition. It is all in memory now. */
public class SliceCache {

  private final List<Slice> slices = new ArrayList<>();

  public Record getOriginalRecord(long index) {
    long previousSize = 0;
    for (Slice slice : slices) {
      long currentSize = slice.getSize();
      if (index < previousSize + currentSize) {
        return slice.getPassThroughRecord((int) (index - previousSize));
      }
      previousSize += currentSize;
    }
    throw new IndexOutOfBoundsException("Index out of bound");
  }

  public void addSlice(Slice slice) {
    slices.add(slice);
  }

  public void clear() {
    slices.clear();
  }

  public void close() {
    // do nothing
  }
}
