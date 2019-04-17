/**
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
package org.apache.iotdb.cluster.query.timegenerator;

import java.io.IOException;
import org.apache.iotdb.cluster.query.manager.coordinatornode.ClusterRpcSingleQueryManager;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.Node;

public class ClusterTimeGenerator implements TimeGenerator {
  private IExpression expression;
  private Node operatorNode;

  /**
   * Constructor of Cluster TimeGenerator.
   */
  public ClusterTimeGenerator(IExpression expression, QueryContext context,
      ClusterRpcSingleQueryManager queryManager)
      throws FileNodeManagerException {
    this.expression = expression;
    initNode(context, queryManager);
  }

  private void initNode(QueryContext context, ClusterRpcSingleQueryManager queryManager)
      throws FileNodeManagerException {
    ClusterNodeConstructor nodeConstructor = new ClusterNodeConstructor(queryManager);
    this.operatorNode = nodeConstructor.construct(expression, context);
  }

  @Override
  public boolean hasNext() throws IOException {
    return operatorNode.hasNext();
  }

  @Override
  public long next() throws IOException {
    return operatorNode.next();
  }

  @Override
  public Object getValue(Path path, long time) throws IOException {
    return null;
  }
}
