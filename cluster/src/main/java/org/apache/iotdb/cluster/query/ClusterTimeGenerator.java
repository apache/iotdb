/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.query;

import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.timegenerator.EngineNodeConstructor;
import org.apache.iotdb.db.query.timegenerator.EngineTimeGenerator;
import org.apache.iotdb.tsfile.read.expression.IExpression;

class ClusterTimeGenerator extends EngineTimeGenerator {

  private MetaGroupMember metaGroupMember;

  /**
   * Constructor of EngineTimeGenerator.
   */
  ClusterTimeGenerator(IExpression expression,
      QueryContext context, MetaGroupMember metaGroupMember) throws StorageEngineException {
    super(expression);
    this.metaGroupMember = metaGroupMember;
    initNode(context);
  }

  @Override
  protected void initNode(QueryContext context) throws StorageEngineException {
    EngineNodeConstructor engineNodeConstructor = new ClusterNodeConstructor(metaGroupMember);
    this.operatorNode = engineNodeConstructor.construct(expression, context);
  }
}
