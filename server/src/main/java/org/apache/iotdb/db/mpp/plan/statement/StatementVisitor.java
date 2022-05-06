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

import org.apache.iotdb.db.mpp.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowsOfOneDeviceStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.QueryStatement;
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
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.binary.AdditionExpression;
import org.apache.iotdb.db.query.expression.binary.BinaryExpression;
import org.apache.iotdb.db.query.expression.binary.DivisionExpression;
import org.apache.iotdb.db.query.expression.binary.EqualToExpression;
import org.apache.iotdb.db.query.expression.binary.GreaterEqualExpression;
import org.apache.iotdb.db.query.expression.binary.GreaterThanExpression;
import org.apache.iotdb.db.query.expression.binary.LessEqualExpression;
import org.apache.iotdb.db.query.expression.binary.LessThanExpression;
import org.apache.iotdb.db.query.expression.binary.LogicAndExpression;
import org.apache.iotdb.db.query.expression.binary.LogicOrExpression;
import org.apache.iotdb.db.query.expression.binary.ModuloExpression;
import org.apache.iotdb.db.query.expression.binary.MultiplicationExpression;
import org.apache.iotdb.db.query.expression.binary.NonEqualExpression;
import org.apache.iotdb.db.query.expression.binary.SubtractionExpression;
import org.apache.iotdb.db.query.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.query.expression.leaf.LeafOperand;
import org.apache.iotdb.db.query.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.query.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.query.expression.multi.FunctionExpression;
import org.apache.iotdb.db.query.expression.unary.InExpression;
import org.apache.iotdb.db.query.expression.unary.LikeExpression;
import org.apache.iotdb.db.query.expression.unary.LogicNotExpression;
import org.apache.iotdb.db.query.expression.unary.NegationExpression;
import org.apache.iotdb.db.query.expression.unary.RegularExpression;
import org.apache.iotdb.db.query.expression.unary.UnaryExpression;

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

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Expression
  /////////////////////////////////////////////////////////////////////////////////////////////////

  public R visitExpression(Expression expression, C context) {
    return visitNode(expression, context);
  }

  public R visitBinaryExpression(BinaryExpression expression, C context) {
    return visitExpression(expression, context);
  }

  public R visitUnaryExpression(UnaryExpression expression, C context) {
    return visitExpression(expression, context);
  }

  public R visitFunctionExpression(FunctionExpression expression, C context) {
    return visitExpression(expression, context);
  }

  public R visitLeafOperand(LeafOperand expression, C context) {
    return visitExpression(expression, context);
  }

  public R visitAdditionExpression(AdditionExpression expression, C context) {
    return visitBinaryExpression(expression, context);
  }

  public R visitDivisionExpression(DivisionExpression expression, C context) {
    return visitBinaryExpression(expression, context);
  }

  public R visitMultiplicationExpression(MultiplicationExpression expression, C context) {
    return visitBinaryExpression(expression, context);
  }

  public R visitSubtractionExpression(SubtractionExpression expression, C context) {
    return visitBinaryExpression(expression, context);
  }

  public R visitModuloExpression(ModuloExpression expression, C context) {
    return visitBinaryExpression(expression, context);
  }

  public R visitEqualToExpression(EqualToExpression expression, C context) {
    return visitBinaryExpression(expression, context);
  }

  public R visitNonEqualExpression(NonEqualExpression expression, C context) {
    return visitBinaryExpression(expression, context);
  }

  public R visitGreaterEqualExpression(GreaterEqualExpression expression, C context) {
    return visitBinaryExpression(expression, context);
  }

  public R visitGreaterThanExpression(GreaterThanExpression expression, C context) {
    return visitBinaryExpression(expression, context);
  }

  public R visitLessEqualExpression(LessEqualExpression expression, C context) {
    return visitBinaryExpression(expression, context);
  }

  public R visitLessThanExpression(LessThanExpression expression, C context) {
    return visitBinaryExpression(expression, context);
  }

  public R visitLogicAndExpression(LogicAndExpression expression, C context) {
    return visitBinaryExpression(expression, context);
  }

  public R visitLogicOrExpression(LogicOrExpression expression, C context) {
    return visitBinaryExpression(expression, context);
  }

  public R visitInExpression(InExpression expression, C context) {
    return visitUnaryExpression(expression, context);
  }

  public R visitLikeExpression(LikeExpression expression, C context) {
    return visitUnaryExpression(expression, context);
  }

  public R visitRegularExpression(RegularExpression expression, C context) {
    return visitUnaryExpression(expression, context);
  }

  public R visitNegationExpression(NegationExpression expression, C context) {
    return visitUnaryExpression(expression, context);
  }

  public R visitLogicNotExpression(LogicNotExpression expression, C context) {
    return visitUnaryExpression(expression, context);
  }

  public R visitTimeSeriesOperand(TimeSeriesOperand expression, C context) {
    return visitLeafOperand(expression, context);
  }

  public R visitTimestampOperand(TimestampOperand expression, C context) {
    return visitLeafOperand(expression, context);
  }

  public R visitConstantOperand(ConstantOperand expression, C context) {
    return visitLeafOperand(expression, context);
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Statement
  /////////////////////////////////////////////////////////////////////////////////////////////////

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
