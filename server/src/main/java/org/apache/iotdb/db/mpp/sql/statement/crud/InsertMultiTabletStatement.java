/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.mpp.sql.statement.crud;

import org.apache.iotdb.commons.partition.TimePartitionSlot;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.schematree.SchemaTree;

import java.util.ArrayList;
import java.util.List;

public class InsertMultiTabletStatement extends InsertBaseStatement {

  /** the InsertTabletStatement list */
  List<InsertTabletStatement> insertTabletStatementList;

  private List<PartialPath> devicePaths;

  public List<InsertTabletStatement> getInsertTabletStatementList() {
    return insertTabletStatementList;
  }

  public void setInsertTabletStatementList(List<InsertTabletStatement> insertTabletStatementList) {
    this.insertTabletStatementList = insertTabletStatementList;
  }

  public List<PartialPath> getPrefixPaths() {
    if (devicePaths != null) {
      return devicePaths;
    }
    devicePaths = new ArrayList<>(insertTabletStatementList.size());
    for (InsertTabletStatement insertTabletStatement : insertTabletStatementList) {
      devicePaths.add(insertTabletStatement.getDevicePath());
    }
    return devicePaths;
  }

  @Override
  public List<TimePartitionSlot> getTimePartitionSlots() {
    return null;
  }

  @Override
  public boolean checkDataType(SchemaTree schemaTree) {
    return false;
  }
}
