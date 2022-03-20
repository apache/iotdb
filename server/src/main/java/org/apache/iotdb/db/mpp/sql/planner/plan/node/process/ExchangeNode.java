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

package org.apache.iotdb.db.mpp.sql.planner.plan.node.process;

import com.google.common.collect.ImmutableList;
import org.apache.iotdb.db.mpp.common.FragmentId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;

import java.util.Collections;
import java.util.List;

public class ExchangeNode extends PlanNode {
    private PlanNode sourceNode;
    private FragmentId sourceFragmentId;

    public ExchangeNode(PlanNodeId id) {
        super(id);
    }

    @Override
    public List<PlanNode> getChildren() {
        return ImmutableList.of(sourceNode);
    }

    @Override
    public PlanNode clone() {
        return new ExchangeNode(getId());
    }

    @Override
    public PlanNode cloneWithChildren(List<PlanNode> children) {
        ExchangeNode node = new ExchangeNode(getId());
        node.setSourceNode(children.get(0));
        return node;
    }

    @Override
    public List<String> getOutputColumnNames() {
        return null;
    }

    public void setSourceFragmentId(FragmentId sourceFragmentId) {
        this.sourceFragmentId = sourceFragmentId;
    }

    public FragmentId getSourceFragmentId() {
        return sourceFragmentId;
    }

    public PlanNode getSourceNode() {
        return sourceNode;
    }

    public void setSourceNode(PlanNode sourceNode) {
        this.sourceNode = sourceNode;
    }

    public String toString() {
        return String.format("ExchangeNode-%s", getId());
    }

}
