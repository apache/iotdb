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
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.common.schematree.SchemaTree;
import org.apache.iotdb.db.mpp.sql.statement.StatementVisitor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.*;

public class InsertRowsOfOneDeviceStatement extends InsertBaseStatement {

  /** the InsertRowsStatement list */
  private List<InsertRowStatement> insertRowStatementList;

  public List<InsertRowStatement> getInsertRowStatementList() {
    return insertRowStatementList;
  }

  public void setInsertRowStatementList(List<InsertRowStatement> insertRowStatementList) {
    this.insertRowStatementList = insertRowStatementList;

    // set device path, measurements, and data types
    if (insertRowStatementList == null || insertRowStatementList.size() == 0) {
      return;
    }
    devicePath = insertRowStatementList.get(0).getDevicePath();
    Map<String, TSDataType> measurementsAndDataType = new HashMap<>();
    for (InsertRowStatement insertRowStatement : insertRowStatementList) {
      List<String> measurements = Arrays.asList(insertRowStatement.getMeasurements());

      //      return measurements.stream().collect(Collectors.toMap(key -> key, key ->
      // insertRowStatement.dataTypes[measurements.indexOf(key)]));
      //      measurementsAndDataType.p；
      //      List a = new ArrayList<>();
      //      a.stream().collect()

    }
  }

  public List<TimePartitionSlot> getTimePartitionSlots() {
    Set<TimePartitionSlot> timePartitionSlotSet = new HashSet<>();
    for (InsertRowStatement insertRowStatement : insertRowStatementList) {
      timePartitionSlotSet.add(StorageEngine.getTimePartitionSlot(insertRowStatement.getTime()));
    }
    return new ArrayList<>(timePartitionSlotSet);
  }

  @Override
  public boolean checkDataType(SchemaTree schemaTree) {
    for (InsertRowStatement insertRowStatement : insertRowStatementList) {
      if (!insertRowStatement.checkDataType(schemaTree)) {
        return false;
      }
    }
    return true;
  }

  public void transferType(SchemaTree schemaTree) throws QueryProcessException {
    for (InsertRowStatement insertRowStatement : insertRowStatementList) {
      insertRowStatement.transferType(schemaTree);
    }
  }

  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitInsertRowsOfOneDevice(this, context);
  }
}
