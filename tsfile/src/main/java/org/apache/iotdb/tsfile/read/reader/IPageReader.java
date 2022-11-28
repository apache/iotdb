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
package org.apache.iotdb.tsfile.read.reader;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.io.IOException;
import java.util.List;

public interface IPageReader {

  default BatchData getAllSatisfiedPageData() throws IOException {
    return getAllSatisfiedPageData(true);
  }

  BatchData getAllSatisfiedPageData(boolean ascending) throws IOException;

  TsBlock getAllSatisfiedData() throws IOException;

  void writeDataToBuilder(TsBlockBuilder builder) throws IOException;

  default void getAllSatisfiedData(boolean ascending, TsBlockBuilder builder) throws IOException {
    if (ascending) {
      writeDataToBuilder(builder);
    } else {
      TimeColumnBuilder timeColumnBuilder = builder.getTimeColumnBuilder();
      ColumnBuilder[] valueColumnBuilders = builder.getValueColumnBuilders();
      int columnNum = valueColumnBuilders.length;

      TsBlock tsBlock = getAllSatisfiedData();
      tsBlock.reverse();
      for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
        timeColumnBuilder.write(tsBlock.getTimeColumn(), i);
        for (int columnIndex = 0; columnIndex < columnNum; columnIndex++) {
          valueColumnBuilders[columnIndex].write(tsBlock.getColumn(columnIndex), i);
        }
        builder.declarePosition();
      }
    }
  }

  Statistics getStatistics();

  void setFilter(Filter filter);

  boolean isModified();

  void initTsBlockBuilder(List<TSDataType> dataTypes);
}
