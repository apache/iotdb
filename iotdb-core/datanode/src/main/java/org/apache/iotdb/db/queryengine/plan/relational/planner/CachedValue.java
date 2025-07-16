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

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;

import org.apache.tsfile.read.common.type.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CachedValue {

  PlanNode planNode;

  DatasetHeader respHeader;
  HashMap<Symbol, Type> symbolMap;
  Map<Symbol, ColumnSchema> assignments;

  // Used for indexScan to fetch device
  List<Expression> metadataExpressionList;
  List<String> attributeColumns;
  List<Literal> literalReference;

  public CachedValue(
      PlanNode planNode,
      List<Literal> literalReference,
      DatasetHeader header,
      HashMap<Symbol, Type> symbolMap,
      Map<Symbol, ColumnSchema> assignments,
      List<Expression> metadataExpressionList,
      List<String> attributeColumns) {
    this.planNode = planNode;
    this.respHeader = header;
    this.symbolMap = symbolMap;
    this.assignments = assignments;
    this.metadataExpressionList = metadataExpressionList;
    this.attributeColumns = attributeColumns;
    this.literalReference = literalReference;
  }

  public DatasetHeader getRespHeader() {
    return respHeader;
  }

  public PlanNode getPlanNode() {
    return planNode;
  }

  public HashMap<Symbol, Type> getSymbolMap() {
    return symbolMap;
  }

  public List<Expression> getMetadataExpressionList() {
    return metadataExpressionList;
  }

  public List<String> getAttributeColumns() {
    return attributeColumns;
  }

  public Map<Symbol, ColumnSchema> getAssignments() {
    return assignments;
  }

  public List<Literal> getLiteralReference() {
    return literalReference;
  }
}
