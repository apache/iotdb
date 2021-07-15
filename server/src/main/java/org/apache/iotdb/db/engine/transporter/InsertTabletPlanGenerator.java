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

package org.apache.iotdb.db.engine.transporter;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.write.record.Tablet;

import java.util.ArrayList;
import java.util.List;

public class InsertTabletPlanGenerator {

  private final QueryDataSet queryDataSet;
  private final List<PartialPath> intoPaths;

  private final String device;
  private final List<Integer> measurementIdIndexes;

  private Tablet tablet;

  public InsertTabletPlanGenerator(
      QueryDataSet queryDataSet, List<PartialPath> intoPaths, String device) {
    this.queryDataSet = queryDataSet;
    this.intoPaths = intoPaths;

    this.device = device;
    measurementIdIndexes = new ArrayList<>();
  }

  public void addMeasurementIdIndex(int measurementIdIndex) {
    measurementIdIndexes.add(measurementIdIndex);
  }

  public void constructNewTablet() {}

  public void collectRowRecord(RowRecord rowRecord) {}

  public InsertTabletPlan getInsertTabletPlan() {
    throw new UnsupportedOperationException();
  }
}
