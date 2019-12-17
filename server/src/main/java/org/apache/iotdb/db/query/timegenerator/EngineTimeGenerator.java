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
package org.apache.iotdb.db.query.timegenerator;

import java.io.IOException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.Node;

/**
 * A timestamp generator for query with filter. e.g. For query clause "select s1, s2 form root where
 * s3 < 0 and time > 100", this class can iterate back to every timestamp of the query.
 */
public class EngineTimeGenerator implements TimeGenerator {

  protected IExpression expression;
  protected Node operatorNode;

  public EngineTimeGenerator(IExpression expression) {
    this.expression = expression;
  }

  /**
   * Constructor of EngineTimeGenerator.
   */
  public EngineTimeGenerator(IExpression expression, QueryContext context)
      throws StorageEngineException {
    this.expression = expression;
    initNode(context);
  }

  protected void initNode(QueryContext context) throws StorageEngineException {
    EngineNodeConstructor engineNodeConstructor = new EngineNodeConstructor();
    this.operatorNode = engineNodeConstructor.construct(expression, context);
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
  public Object getValue(Path path, long time) {
    return null;
  }

}
