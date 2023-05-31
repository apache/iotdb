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

package org.apache.iotdb.db.pipe.core.event.view.access;

import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class PipeRowIterator implements Iterator<Row>, Iterable<Row> {
  private final List<Path> columnNameList;
  private final List<TSDataType> columnTypeList;
  private final long[] timestamps;
  private final Object[][] rowRecords;
  private int currentIndex;
  private final int endIndex;

  public PipeRowIterator(
      List<Path> columnNameList,
      List<TSDataType> columnTypeList,
      long[] timestamps,
      Object[][] rowRecords,
      int startIndex,
      int endIndex) {
    this.columnNameList = columnNameList;
    this.columnTypeList = columnTypeList;
    this.timestamps = timestamps;
    this.rowRecords = rowRecords;
    this.currentIndex = startIndex;
    this.endIndex = endIndex;
  }

  @Override
  public boolean hasNext() {
    return currentIndex < endIndex;
  }

  @Override
  public Row next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    Row row =
        new PipeRow(columnNameList, columnTypeList, timestamps[currentIndex])
            .setRowRecord(rowRecords[currentIndex]);
    currentIndex++;
    return row;
  }

  @Override
  public Iterator<Row> iterator() {
    return this;
  }
}
