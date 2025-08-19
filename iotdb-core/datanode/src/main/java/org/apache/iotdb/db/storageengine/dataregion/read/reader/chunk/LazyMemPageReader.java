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

package org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.reader.IPageReader;
import org.apache.tsfile.read.reader.series.PaginationController;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;

public class LazyMemPageReader implements IPageReader {

  @Override
  public BatchData getAllSatisfiedPageData(boolean b) throws IOException {
    return null;
  }

  @Override
  public TsBlock getAllSatisfiedData() throws IOException {
    return null;
  }

  @Override
  public void addRecordFilter(Filter filter) {}

  @Override
  public boolean isModified() {
    return false;
  }

  @Override
  public void initTsBlockBuilder(List<TSDataType> list) {}

  @Override
  public void setLimitOffset(PaginationController paginationController) {}

  @Override
  public Statistics<? extends Serializable> getStatistics() {
    return null;
  }

  @Override
  public Statistics<? extends Serializable> getTimeStatistics() {
    return null;
  }

  @Override
  public Optional<Statistics<? extends Serializable>> getMeasurementStatistics(int i) {
    return Optional.empty();
  }

  @Override
  public boolean hasNullValue(int i) {
    return false;
  }
}
