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

package org.apache.iotdb.db.mpp.sql.statement;

import org.apache.iotdb.db.mpp.sql.statement.crud.*;
import org.apache.iotdb.db.mpp.sql.statement.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.ShowDevicesStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.ShowTimeSeriesStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.SchemaFetchStatement;
import org.apache.iotdb.db.mpp.sql.statement.sys.AuthorStatement;

/**
 * This class provides a visitor of {@link org.apache.iotdb.db.mpp.sql.statement.StatementNode},
 * which can be extended to create a visitor which only needs to handle a subset of the available
 * methods.
 *
 * @param <R> The return type of the visit operation.
 * @param <C> The context information during visiting.
 */
public abstract class StatementVisitor<R, C> {

  public R process(StatementNode node, C context) {
    return node.accept(this, context);
  }

  /** Top Level Description */
  public R visitNode(StatementNode node, C context) {
    return null;
  }

  public R visitStatement(Statement statement, C context) {
    return visitNode(statement, context);
  }

  /** Data Definition Language (DDL) */

  // Create Timeseries
  public R visitCreateTimeseries(CreateTimeSeriesStatement createTimeSeriesStatement, C context) {
    return visitStatement(createTimeSeriesStatement, context);
  }

  // Create Aligned Timeseries
  public R visitCreateAlignedTimeseries(
      CreateAlignedTimeSeriesStatement createAlignedTimeSeriesStatement, C context) {
    return visitStatement(createAlignedTimeSeriesStatement, context);
  }

  // Alter Timeseries
  public R visitAlterTimeseries(AlterTimeSeriesStatement alterTimeSeriesStatement, C context) {
    return visitStatement(alterTimeSeriesStatement, context);
  }

  /** Data Manipulation Language (DML) */

  // Select Statement
  public R visitQuery(QueryStatement queryStatement, C context) {
    return visitStatement(queryStatement, context);
  }

  public R visitAggregationQuery(AggregationQueryStatement queryStatement, C context) {
    return visitQuery(queryStatement, context);
  }

  public R visitFillQuery(FillQueryStatement queryStatement, C context) {
    return visitQuery(queryStatement, context);
  }

  public R visitGroupByQuery(GroupByQueryStatement queryStatement, C context) {
    return visitQuery(queryStatement, context);
  }

  public R visitGroupByFillQuery(GroupByFillQueryStatement queryStatement, C context) {
    return visitQuery(queryStatement, context);
  }

  public R visitLastQuery(LastQueryStatement queryStatement, C context) {
    return visitQuery(queryStatement, context);
  }

  public R visitUDTFQuery(UDTFQueryStatement queryStatement, C context) {
    return visitQuery(queryStatement, context);
  }

  public R visitUDAFQuery(UDAFQueryStatement queryStatement, C context) {
    return visitQuery(queryStatement, context);
  }

  // Insert Statement
  public R visitInsert(InsertStatement insertStatement, C context) {
    return visitStatement(insertStatement, context);
  }

  public R visitInsertTablet(InsertTabletStatement insertTabletStatement, C context) {
    return visitStatement(insertTabletStatement, context);
  }

  /** Data Control Language (DCL) */

  // Create User
  public R visitCreateUser(AuthorStatement authorStatement, C context) {
    return visitStatement(authorStatement, context);
  }

  // Create Role
  public R visitCreateRole(AuthorStatement authorStatement, C context) {
    return visitStatement(authorStatement, context);
  }

  // Alter Password
  public R visitAlterUser(AuthorStatement authorStatement, C context) {
    return visitStatement(authorStatement, context);
  }

  // Grant User Privileges
  public R visitGrantUser(AuthorStatement authorStatement, C context) {
    return visitStatement(authorStatement, context);
  }

  // Grant Role Privileges
  public R visitGrantRole(AuthorStatement authorStatement, C context) {
    return visitStatement(authorStatement, context);
  }

  // Grant User Role
  public R visitGrantRoleToUser(AuthorStatement authorStatement, C context) {
    return visitStatement(authorStatement, context);
  }

  // Revoke User Privileges
  public R visitRevokeUser(AuthorStatement authorStatement, C context) {
    return visitStatement(authorStatement, context);
  }

  // Revoke Role Privileges
  public R visitRevokeRole(AuthorStatement authorStatement, C context) {
    return visitStatement(authorStatement, context);
  }

  // Revoke Role From User
  public R visitRevokeRoleFromUser(AuthorStatement authorStatement, C context) {
    return visitStatement(authorStatement, context);
  }

  // Drop User
  public R visitDropUser(AuthorStatement authorStatement, C context) {
    return visitStatement(authorStatement, context);
  }

  // Drop Role
  public R visitDropRole(AuthorStatement authorStatement, C context) {
    return visitStatement(authorStatement, context);
  }

  // List Users
  public R visitListUser(AuthorStatement authorStatement, C context) {
    return visitStatement(authorStatement, context);
  }

  // List Roles
  public R visitListRole(AuthorStatement authorStatement, C context) {
    return visitStatement(authorStatement, context);
  }

  // List Privileges
  public R visitListPrivilegesUser(AuthorStatement authorStatement, C context) {
    return visitStatement(authorStatement, context);
  }

  // List Privileges of Roles On Specific Path
  public R visitListPrivilegesRole(AuthorStatement authorStatement, C context) {
    return visitStatement(authorStatement, context);
  }

  // List Privileges of Users
  public R visitListUserPrivileges(AuthorStatement authorStatement, C context) {
    return visitStatement(authorStatement, context);
  }

  // List Privileges of Roles
  public R visitListRolePrivileges(AuthorStatement authorStatement, C context) {
    return visitStatement(authorStatement, context);
  }

  // List Roles of Users
  public R visitListAllRoleOfUser(AuthorStatement authorStatement, C context) {
    return visitStatement(authorStatement, context);
  }

  // List Users of Role
  public R visitListAllUserOfRole(AuthorStatement authorStatement, C context) {
    return visitStatement(authorStatement, context);
  }

  public R visitShowTimeSeries(ShowTimeSeriesStatement showTimeSeriesStatement, C context) {
    return visitStatement(showTimeSeriesStatement, context);
  }

  public R visitShowDevices(ShowDevicesStatement showDevicesStatement, C context) {
    return visitStatement(showDevicesStatement, context);
  }

  public R visitInsertRow(InsertRowStatement insertRowStatement, C context) {
    return visitStatement(insertRowStatement, context);
  }

  public R visitInsertRows(InsertRowsStatement insertRowsStatement, C context) {
    return visitStatement(insertRowsStatement, context);
  }

  public R visitInsertMultiTablets(
      InsertMultiTabletsStatement insertMultiTabletsStatement, C context) {
    return visitStatement(insertMultiTabletsStatement, context);
  }

  public R visitInsertRowsOfOneDevice(
      InsertRowsOfOneDeviceStatement insertRowsOfOneDeviceStatement, C context) {
    return visitStatement(insertRowsOfOneDeviceStatement, context);
  }

  public R visitSchemaFetch(SchemaFetchStatement schemaFetchStatement, C context) {
    return visitStatement(schemaFetchStatement, context);
  }
}
