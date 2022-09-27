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

package org.apache.iotdb.db.sync.externalpipe.operation;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/** An insert operation may contain multiple insertions to multiple timeseries. */
public class InsertOperation extends Operation {
  private static final Logger logger = LoggerFactory.getLogger(InsertOperation.class);

  // May save multiple different Measurements, and every Measurement may save many TimeValuePairs
  private final List<Pair<MeasurementPath, List<TimeValuePair>>> dataList;

  public InsertOperation(
      String storageGroup,
      long startIndex,
      long endIndex,
      List<Pair<MeasurementPath, List<TimeValuePair>>> dataList) {
    super(OperationType.INSERT, storageGroup, startIndex, endIndex);
    this.dataList = Validate.notNull(dataList);
  }

  public List<Pair<MeasurementPath, List<TimeValuePair>>> getDataList() {
    return dataList;
  }

  @Override
  public String toString() {
    String tmpStr = "";
    if (logger.isDebugEnabled()) {
      tmpStr = ", dataList=" + dataList;
    }

    return "InsertOperation{" + super.toString() + tmpStr + '}';
  }
}
