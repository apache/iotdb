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
package org.apache.iotdb.db.qp.physical.crud;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

import java.time.ZoneId;
import java.util.HashSet;
import java.util.Set;

public class UDAFPlan extends UDTFPlan {

  // Construct an innerAggregationPlan using resultColumns of UDAFPlan
  private AggregationPlan innerAggregationPlan;

  public UDAFPlan(ZoneId zoneId) {
    super(zoneId);
    setOperatorType(OperatorType.UDAF);
  }

  public void setInnerAggregationPlan(AggregationPlan innerAggregationPlan) {
    this.innerAggregationPlan = innerAggregationPlan;
  }

  public AggregationPlan getInnerAggregationPlan() {
    return innerAggregationPlan;
  }

  @Override
  public void deduplicate(PhysicalGenerator physicalGenerator) throws MetadataException {
    Set<String> columnForDisplaySet = new HashSet<>();
    for (int i = 0; i < resultColumns.size(); i++) {
      String columnForDisplay = resultColumns.get(i).getResultColumnName();
      if (!columnForDisplaySet.contains(columnForDisplay)) {
        int datasetOutputIndex = getPathToIndex().size();
        setColumnNameToDatasetOutputIndex(columnForDisplay, datasetOutputIndex);
        setDatasetOutputIndexToResultColumnIndex(datasetOutputIndex, i);
        columnForDisplaySet.add(columnForDisplay);
      }
    }
  }
}
