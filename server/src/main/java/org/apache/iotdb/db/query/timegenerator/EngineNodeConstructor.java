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

import static org.apache.iotdb.tsfile.read.expression.ExpressionType.AND;
import static org.apache.iotdb.tsfile.read.expression.ExpressionType.OR;
import static org.apache.iotdb.tsfile.read.expression.ExpressionType.SERIES;

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.path.PathException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IBinaryExpression;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.AndNode;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.Node;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.OrNode;

public class EngineNodeConstructor {

  /**
   * Construct expression node.
   *
   * @param expression expression
   * @return Node object
   * @throws StorageEngineException StorageEngineException
   */
  public Node construct(IExpression expression, QueryContext context)
      throws StorageEngineException {
    if (expression.getType() == SERIES) {
      try {
        Filter filter = ((SingleSeriesExpression) expression).getFilter();
        Path path = ((SingleSeriesExpression) expression).getSeriesPath();
        TSDataType dataType = MManager.getInstance().getSeriesType(path.getFullPath());

        QueryDataSource queryDataSource = QueryResourceManager.getInstance()
            .getQueryDataSource(path, context, null);
        // update filter by TTL
        filter = queryDataSource.updateFilterUsingTTL(filter);

        return new EngineLeafNode(
            new SeriesRawDataBatchReader(path, dataType, context, queryDataSource, null, filter));
      } catch (PathException e) {
        throw new StorageEngineException(e.getMessage());
      }
    } else {
      IBinaryExpression binaryExpression = (IBinaryExpression) expression;
      Node leftChild = construct(binaryExpression.getLeft(), context);
      Node rightChild = construct(binaryExpression.getRight(), context);

      if (expression.getType() == OR) {
        return new OrNode(leftChild, rightChild);
      } else if (expression.getType() == AND) {
        return new AndNode(leftChild, rightChild);
      } else {
        throw new UnSupportedDataTypeException(
            "Unsupported QueryFilterType when construct OperatorNode: " + expression.getType());
      }
    }
  }
}
