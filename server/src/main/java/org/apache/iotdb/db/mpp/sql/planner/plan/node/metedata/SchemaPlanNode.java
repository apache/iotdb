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

package org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.Executor.ISchemaQueryExecutor;
import org.apache.iotdb.db.metadata.schemaregion.SchemaRegion;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import java.nio.ByteBuffer;
import java.util.List;

/** SchemaPlanNode implements ISchemaQueryExecutor for schema read and write operation. */
public class SchemaPlanNode extends PlanNode implements ISchemaQueryExecutor {
  protected SchemaPlanNode(PlanNodeId id) {
    super(id);
  }

  @Override
  public void executeOn(SchemaRegion schemaRegion)
      throws MetadataException, QueryProcessException {}

  @Override
  public QueryDataSet queryOn(SchemaRegion schemaRegion) {
    return null;
  }

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    return null;
  }

  @Override
  public int allowedChildCount() {
    return 0;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {}

  public PhysicalPlan transferToPhysicalPlan() {
    return null;
  }
}
