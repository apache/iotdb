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

package org.apache.iotdb.db.mpp.plan.statement;

import org.apache.iotdb.db.mpp.plan.statement.crud.AggregationQueryStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.FillQueryStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.GroupByFillQueryStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.GroupByQueryStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowsOfOneDeviceStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.LastQueryStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.UDAFQueryStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.UDTFQueryStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountDevicesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountLevelTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.SchemaFetchStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.SetStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.SetTTLStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowDevicesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowTTLStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.UnSetTTLStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.AuthorStatement;

/**
 * This class provides a visitor of {@link org.apache.iotdb.db.mpp.plan.statement.StatementNode},
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
  public abstract R visitNode(StatementNode node, C context);

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

  public R visitSetStorageGroup(SetStorageGroupStatement alterTimeSeriesStatement, C context) {
    return visitStatement(alterTimeSeriesStatement, context);
  }

  // Alter TTL
  public R visitSetTTL(SetTTLStatement setTTLStatement, C context) {
    return visitStatement(setTTLStatement, context);
  }

  public R visitUnSetTTL(UnSetTTLStatement unSetTTLStatement, C context) {
    return visitStatement(unSetTTLStatement, context);
  }

  public R visitShowTTL(ShowTTLStatement showTTLStatement, C context) {
    return visitStatement(showTTLStatement, context);
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
  public R visitAuthor(AuthorStatement authorStatement, C context) {
    return visitStatement(authorStatement, context);
  }

  public R visitShowStorageGroup(ShowStorageGroupStatement showStorageGroupStatement, C context) {
    return visitStatement(showStorageGroupStatement, context);
  }

  public R visitShowTimeSeries(ShowTimeSeriesStatement showTimeSeriesStatement, C context) {
    return visitStatement(showTimeSeriesStatement, context);
  }

  public R visitShowDevices(ShowDevicesStatement showDevicesStatement, C context) {
    return visitStatement(showDevicesStatement, context);
  }

  public R visitCountStorageGroup(
      CountStorageGroupStatement countStorageGroupStatement, C context) {
    return visitStatement(countStorageGroupStatement, context);
  }

  public R visitCountDevices(CountDevicesStatement countStatement, C context) {
    return visitStatement(countStatement, context);
  }

  public R visitCountTimeSeries(CountTimeSeriesStatement countStatement, C context) {
    return visitStatement(countStatement, context);
  }

  public R visitCountLevelTimeSeries(CountLevelTimeSeriesStatement countStatement, C context) {
    return visitStatement(countStatement, context);
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
