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

package org.apache.iotdb.db.mpp.plan.execution.memory;

import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.common.header.HeaderConstant;
import org.apache.iotdb.db.mpp.plan.statement.StatementNode;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountNodesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowChildNodesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowChildPathsStatement;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;

public class StatementMemorySourceVisitor
    extends StatementVisitor<StatementMemorySource, StatementMemorySourceContext> {

  @Override
  public StatementMemorySource visitNode(StatementNode node, StatementMemorySourceContext context) {
    return new StatementMemorySource(new TsBlock(0), new DatasetHeader(new ArrayList<>(), false));
  }

  @Override
  public StatementMemorySource visitShowChildPaths(
      ShowChildPathsStatement showChildPathsStatement, StatementMemorySourceContext context) {
    TsBlockBuilder tsBlockBuilder =
        new TsBlockBuilder(HeaderConstant.showChildPathsHeader.getRespDataTypes());
    Set<String> matchedChildPaths = new TreeSet<>(context.getAnalysis().getMatchedNodes());
    matchedChildPaths.forEach(
        path -> {
          tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
          tsBlockBuilder.getColumnBuilder(0).writeBinary(new Binary(path));
          tsBlockBuilder.declarePosition();
        });
    return new StatementMemorySource(
        tsBlockBuilder.build(), context.getAnalysis().getRespDatasetHeader());
  }

  @Override
  public StatementMemorySource visitShowChildNodes(
      ShowChildNodesStatement showChildNodesStatement, StatementMemorySourceContext context) {
    TsBlockBuilder tsBlockBuilder =
        new TsBlockBuilder(HeaderConstant.showChildNodesHeader.getRespDataTypes());
    Set<String> matchedChildNodes = new TreeSet<>(context.getAnalysis().getMatchedNodes());
    matchedChildNodes.forEach(
        node -> {
          tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
          tsBlockBuilder.getColumnBuilder(0).writeBinary(new Binary(node));
          tsBlockBuilder.declarePosition();
        });
    return new StatementMemorySource(
        tsBlockBuilder.build(), context.getAnalysis().getRespDatasetHeader());
  }

  @Override
  public StatementMemorySource visitCountNodes(
      CountNodesStatement countStatement, StatementMemorySourceContext context) {
    TsBlockBuilder tsBlockBuilder =
        new TsBlockBuilder(HeaderConstant.countNodesHeader.getRespDataTypes());
    Set<String> matchedChildNodes = new TreeSet<>(context.getAnalysis().getMatchedNodes());
    tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
    tsBlockBuilder.getColumnBuilder(0).writeInt(matchedChildNodes.size());
    tsBlockBuilder.declarePosition();
    return new StatementMemorySource(
        tsBlockBuilder.build(), context.getAnalysis().getRespDatasetHeader());
  }
}
