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

package org.apache.iotdb.db.query.dataset.groupby;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class GroupByLevelDataSet extends QueryDataSet {

  private static final Logger logger = LoggerFactory.getLogger(GroupByLevelDataSet.class);
  private List<RowRecord> records = new ArrayList<>();
  private int index = 0;

  public GroupByLevelDataSet(GroupByTimePlan plan, GroupByEngineDataSet dataSet)
      throws IOException {
    this.paths = new ArrayList<>(plan.getDeduplicatedPaths());
    this.dataTypes = plan.getDeduplicatedDataTypes();

    if (logger.isDebugEnabled()) {
      logger.debug("paths " + this.paths + " level:" + Arrays.toString(plan.getLevels()));
    }

    // get all records from GroupByDataSet, then we merge every record
    if (logger.isDebugEnabled()) {
      logger.debug("only group by level, paths:" + plan.getPaths());
    }

    this.paths = new ArrayList<>();
    this.dataTypes = new ArrayList<>();
    Map<String, AggregateResult> groupPathResultMap;
    while (dataSet != null && dataSet.hasNextWithoutConstraint()) {
      RowRecord rawRecord = dataSet.nextWithoutConstraint();
      RowRecord curRecord = new RowRecord(rawRecord.getTimestamp());
      groupPathResultMap =
          plan.groupAggResultByLevel(Arrays.asList(dataSet.getCurAggregateResults()));
      for (AggregateResult resultData : groupPathResultMap.values()) {
        curRecord.addField(resultData.getResult(), resultData.getResultDataType());
      }
      records.add(curRecord);

      if (paths.isEmpty()) {
        for (Map.Entry<String, AggregateResult> entry : groupPathResultMap.entrySet()) {
          try {
            this.paths.add(new PartialPath(entry.getKey()));
          } catch (IllegalPathException e) {
            logger.error("Query result IllegalPathException occurred: {}.", entry.getKey());
          }
          this.dataTypes.add(entry.getValue().getResultDataType());
        }
      }
    }
  }

  @Override
  public boolean hasNextWithoutConstraint() {
    return index < records.size();
  }

  @Override
  public RowRecord nextWithoutConstraint() {
    return records.get(index++);
  }
}
