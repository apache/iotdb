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
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.series.PaginationController;

import java.io.IOException;
import java.util.List;

public interface IPageReader {

  default BatchData getAllSatisfiedPageData() throws IOException {
    return getAllSatisfiedPageData(true);
  }

  BatchData getAllSatisfiedPageData(boolean ascending) throws IOException;

  TsBlock getAllSatisfiedData() throws IOException;

  Statistics getStatistics();

  void setFilter(Filter filter);

  boolean isModified();

  void initTsBlockBuilder(List<TSDataType> dataTypes);

  void setLimitOffset(PaginationController paginationController);
}
