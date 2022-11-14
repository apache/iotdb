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

package org.apache.iotdb.db.mpp.plan.planner.plan.node.load;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.mpp.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.tsfile.exception.NotImplementedException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class LoadTsFileNode extends WritePlanNode {
  private static final Logger logger = LoggerFactory.getLogger(LoadTsFileNode.class);

  private final List<TsFileResource> resources;

  public LoadTsFileNode(PlanNodeId id) {
    this(id, new ArrayList<>());
  }

  public LoadTsFileNode(PlanNodeId id, List<TsFileResource> resources) {
    super(id);
    this.resources = resources;
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
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
    throw new NotImplementedException("clone of load TsFile is not implemented");
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {}

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {}

  @Override
  public List<WritePlanNode> splitByPartition(Analysis analysis) {
    List<WritePlanNode> res = new ArrayList<>();
    LoadTsFileStatement statement = (LoadTsFileStatement) analysis.getStatement();
    for (TsFileResource resource : resources) {
      try {
        LoadSingleTsFileNode singleTsFileNode =
            new LoadSingleTsFileNode(
                getPlanNodeId(),
                resource,
                statement.isDeleteAfterLoad(),
                analysis.getDataPartitionInfo());
        singleTsFileNode.checkIfNeedDecodeTsFile();
        res.add(singleTsFileNode);
      } catch (Exception e) {
        logger.error(
            String.format(
                "Check whether TsFile %s need decode or not error", resource.getTsFile().getPath()),
            e);
      }
    }
    return res;
  }
}
