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

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;

import java.util.ArrayList;
import java.util.List;

public abstract class ShowDataSet extends QueryDataSet {
  protected ShowPlan plan;
  private List<RowRecord> result = new ArrayList<>();
  private int index = 0;
  protected boolean hasLimit;

  protected ShowDataSet(List<Path> paths, List<TSDataType> dataTypes) {
    super(paths, dataTypes);
  }

  @Override
  public boolean hasNextWithoutConstraint() {
    return index < result.size();
  }

  public abstract List<RowRecord> getQueryDataSet() throws MetadataException;

  @Override
  public RowRecord nextWithoutConstraint() {
    return result.get(index++);
  }

  protected void updateRecord(RowRecord record, String s) {
    if (s == null) {
      record.addField(null);
      return;
    }
    Field field = new Field(TSDataType.TEXT);
    field.setBinaryV(new Binary(s));
    record.addField(field);
  }

  protected void putRecord(RowRecord newRecord) {
    result.add(newRecord);
  }
}
