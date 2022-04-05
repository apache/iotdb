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

import java.util.List;

public class InsertRowsStatement extends InsertBaseStatement {

  /**
   * Suppose there is an InsertRowsStatement, which contains 5 InsertRowStatements,
   * insertRowStatementList={InsertRowStatement_0, InsertRowStatement_1, InsertRowStatement_2,
   * InsertRowStatement_3, InsertRowStatement_4}, then the insertRowStatementIndexList={0, 1, 2, 3,
   * 4} respectively. But when the InsertRowsStatement is split into two InsertRowsStatements
   * according to different storage group in cluster version, suppose that the
   * InsertRowsStatement_1's insertRowStatementList = {InsertRowStatement_0, InsertRowStatement_3,
   * InsertRowStatement_4}, then InsertRowsStatement_1's insertRowStatementIndexList = {0, 3, 4};
   * InsertRowsStatement_2's insertRowStatementList = {InsertRowStatement_1, * InsertRowStatement_2}
   * then InsertRowsStatement_2's insertRowStatementIndexList= {1, 2} respectively;
   */
  private List<Integer> insertRowStatementIndexList;

  /** the InsertRowsStatement list */
  private List<InsertRowStatement> insertRowStatementList;

  private List<PartialPath> devicePaths;

  public List<PartialPath> getDevicePaths() {
    return devicePaths;
  }

  public List<Integer> getInsertRowStatementIndexList() {
    return insertRowStatementIndexList;
  }

  public void setInsertRowStatementIndexList(List<Integer> insertRowStatementIndexList) {
    this.insertRowStatementIndexList = insertRowStatementIndexList;
  }

  public List<InsertRowStatement> getInsertRowStatementList() {
    return insertRowStatementList;
  }

  public void setInsertRowStatementList(List<InsertRowStatement> insertRowStatementList) {
    this.insertRowStatementList = insertRowStatementList;
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
