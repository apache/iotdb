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

package org.apache.iotdb.db.query.timegenerator;

import static org.apache.iotdb.tsfile.read.expression.ExpressionType.SERIES;

import java.io.IOException;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.Node;

public class EngineNodeConstructor extends AbstractNodeConstructor {

  public EngineNodeConstructor() {
  }

  /**
   * Construct expression node.
   *
   * @param expression expression
   * @return Node object
   * @throws IOException IOException
   * @throws FileNodeManagerException FileNodeManagerException
   */
  @Override
  public Node construct(IExpression expression, QueryContext context)
      throws FileNodeManagerException {
    if (expression.getType() == SERIES) {
      try {
        return new EngineLeafNode(generateSeriesReader((SingleSeriesExpression) expression,
            context));
      } catch (IOException e) {
        throw new FileNodeManagerException(e);
      }
    } else {
      return constructNotSeriesNode(expression, context);
    }
  }
}
