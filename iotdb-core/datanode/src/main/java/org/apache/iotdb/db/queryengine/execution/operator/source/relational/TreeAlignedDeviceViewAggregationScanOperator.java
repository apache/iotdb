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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational;

import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;

import org.apache.tsfile.file.metadata.IDeviceID;

public class TreeAlignedDeviceViewAggregationScanOperator
    extends AbstractDefaultAggTableScanOperator {

  private final IDeviceID.TreeDeviceIdColumnValueExtractor extractor;

  public TreeAlignedDeviceViewAggregationScanOperator(
      AbstractAggTableScanOperatorParameter parameter,
      IDeviceID.TreeDeviceIdColumnValueExtractor extractor) {
    super(parameter);
    this.extractor = extractor;
  }

  @Override
  String getNthIdColumnValue(DeviceEntry deviceEntry, int idColumnIndex) {
    return (String) extractor.extract(deviceEntry.getDeviceID(), idColumnIndex);
  }
}
