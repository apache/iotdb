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

package org.apache.iotdb.db.query.dataset;

import java.util.List;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;

public abstract class UDTFDataSet extends QueryDataSet {

  protected final boolean withValueFilter;

  protected final UDTFPlan udtfPlan;

  protected final List<Path> deduplicatedPaths;
  protected final List<TSDataType> deduplicatedDataTypes;

  // not null when data set executes with value filter
  protected TimeGenerator timestampGenerator;
  protected List<IReaderByTimestamp> readersByTimestamp;
  protected List<Boolean> cached;

  // not null when data set executes without value filter
  protected List<ManagedSeriesReader> managedSeriesReaders;

  // execute with value filter
  public UDTFDataSet(UDTFPlan udtfPlan, List<Path> deduplicatedPaths,
      List<TSDataType> deduplicatedDataTypes, TimeGenerator timestampGenerator,
      List<IReaderByTimestamp> readersOfSelectedSeries, List<Boolean> cached) {
    withValueFilter = true;
    this.udtfPlan = udtfPlan;
    this.deduplicatedPaths = deduplicatedPaths;
    this.deduplicatedDataTypes = deduplicatedDataTypes;
    this.timestampGenerator = timestampGenerator;
    this.readersByTimestamp = readersOfSelectedSeries;
    this.cached = cached;
  }

  // execute without value filter
  public UDTFDataSet(UDTFPlan udtfPlan, List<Path> deduplicatedPaths,
      List<TSDataType> deduplicatedDataTypes, List<ManagedSeriesReader> readersOfSelectedSeries) {
    withValueFilter = false;
    this.udtfPlan = udtfPlan;
    this.deduplicatedPaths = deduplicatedPaths;
    this.deduplicatedDataTypes = deduplicatedDataTypes;
    this.managedSeriesReaders = readersOfSelectedSeries;
  }
}
