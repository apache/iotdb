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

package org.apache.iotdb.db.mpp.transformation.dag.adapter;

import org.apache.iotdb.db.mpp.transformation.datastructure.tv.ElasticSerializableTVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.access.RowIterator;
import org.apache.iotdb.udf.api.access.RowWindow;

public class ElasticSerializableTVListBackedSingleColumnWindow implements RowWindow {

  private final ElasticSerializableTVList tvList;
  private int beginIndex;
  private int endIndex;
  private int size;

  private long startTime;
  private long endTime;

  private final ElasticSerializableTVListBackedSingleColumnRow row;
  private ElasticSerializableTVListBackedSingleColumnWindowIterator rowIterator;

  // [beginIndex, endIndex)
  public ElasticSerializableTVListBackedSingleColumnWindow(ElasticSerializableTVList tvList) {
    this.tvList = tvList;
    beginIndex = 0;
    endIndex = 0;
    size = 0;

    row = new ElasticSerializableTVListBackedSingleColumnRow(tvList, beginIndex);
  }

  @Override
  public int windowSize() {
    return size;
  }

  @Override
  public Row getRow(int rowIndex) {
    return row.seek(beginIndex + rowIndex);
  }

  @Override
  public TSDataType getDataType(int columnIndex) {
    return tvList.getDataType();
  }

  @Override
  public RowIterator getRowIterator() {
    if (rowIterator == null) {
      rowIterator =
          new ElasticSerializableTVListBackedSingleColumnWindowIterator(
              tvList, beginIndex, endIndex);
    }

    rowIterator.reset();
    return rowIterator;
  }

  @Override
  public long windowStartTime() {
    return startTime;
  }

  @Override
  public long windowEndTime() {
    return endTime;
  }

  public void seek(int beginIndex, int endIndex, long startTime, long endTime) {
    this.beginIndex = beginIndex;
    this.endIndex = endIndex;
    size = endIndex - beginIndex;

    this.startTime = startTime;
    this.endTime = endTime;

    row.seek(beginIndex);
    rowIterator = null;
  }
}
