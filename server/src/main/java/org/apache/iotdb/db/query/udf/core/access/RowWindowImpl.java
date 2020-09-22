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

package org.apache.iotdb.db.query.udf.core.access;

import java.io.IOException;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.access.RowIterator;
import org.apache.iotdb.db.query.udf.api.access.RowWindow;
import org.apache.iotdb.db.query.udf.datastructure.row.ElasticSerializableRowRecordList;
import org.apache.iotdb.db.query.udf.datastructure.primitive.IntList;

public class RowWindowImpl implements RowWindow {

  private final ElasticSerializableRowRecordList rowRecordList;

  private final RowImpl row;
  private final RowIteratorImpl rowIterator;

  private IntList windowRowIndexes;

  public RowWindowImpl(ElasticSerializableRowRecordList rowRecordList, int[] columnIndexes) {
    this.rowRecordList = rowRecordList;
    row = new RowImpl(columnIndexes);
    rowIterator = new RowIteratorImpl(rowRecordList, columnIndexes);
  }

  @Override
  public int windowSize() {
    return windowRowIndexes.size();
  }

  @Override
  public Row getRow(int index) throws IOException {
    return row.setRowRecord(rowRecordList.getRowRecord(windowRowIndexes.get(index)));
  }

  @Override
  public RowIterator getRowIterator() {
    return rowIterator.set(windowRowIndexes);
  }

  public RowWindowImpl set(IntList windowRowIndexes) {
    this.windowRowIndexes = windowRowIndexes;
    return this;
  }
}
