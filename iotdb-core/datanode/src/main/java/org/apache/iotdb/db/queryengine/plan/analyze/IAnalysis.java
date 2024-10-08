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

package org.apache.iotdb.db.queryengine.plan.analyze;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.plan.planner.plan.TimePredicate;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;

import org.apache.tsfile.read.common.block.TsBlock;

import java.util.List;

public interface IAnalysis {

  boolean isFailed();

  TSStatus getFailStatus();

  boolean canSkipExecute(MPPQueryContext context);

  TsBlock constructResultForMemorySource(MPPQueryContext context);

  boolean isQuery();

  boolean needSetHighestPriority();

  DatasetHeader getRespDatasetHeader();

  String getStatementType();

  void setFinishQueryAfterAnalyze(boolean b);

  void setFailStatus(TSStatus status);

  boolean isFinishQueryAfterAnalyze();

  default void setRealStatement(Statement realStatement) {}

  void setDataPartitionInfo(DataPartition dataPartition);

  SchemaPartition getSchemaPartitionInfo();

  void setSchemaPartitionInfo(SchemaPartition schemaPartition);

  DataPartition getDataPartitionInfo();

  List<TEndPoint> getRedirectNodeList();

  void setRedirectNodeList(List<TEndPoint> redirectNodeList);

  void addEndPointToRedirectNodeList(TEndPoint endPoint);

  TimePredicate getCovertedTimePredicate();

  void setDatabaseName(String databaseName);

  String getDatabaseName();
}
