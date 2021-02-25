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

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import java.util.ArrayList;
import java.util.List;

public class SingleDataSet extends QueryDataSet {

  private RowRecord record;
  private int i = 0;

  public SingleDataSet(List<PartialPath> paths, List<TSDataType> dataTypes) {
    super(new ArrayList<>(paths), dataTypes);
  }

  public void setRecord(RowRecord record) {
    this.record = record;
  }

  @Override
  public boolean hasNextWithoutConstraint() {
    return i == 0;
  }

  @Override
  public RowRecord nextWithoutConstraint() {
    i++;
    return record;
  }
}
