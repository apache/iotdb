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
package org.apache.iotdb.cluster.query.utils;

import static org.apache.iotdb.tsfile.read.expression.ExpressionType.AND;
import static org.apache.iotdb.tsfile.read.expression.ExpressionType.OR;
import static org.apache.iotdb.tsfile.read.expression.ExpressionType.SERIES;
import static org.apache.iotdb.tsfile.read.expression.ExpressionType.TRUE;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.cluster.query.expression.TrueExpression;
import org.apache.iotdb.cluster.utils.QPExecutorUtils;
import org.apache.iotdb.cluster.utils.hash.Router;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.ExpressionType;
import org.apache.iotdb.tsfile.read.expression.IBinaryExpression;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;

public class ExpressionUtils {

  private ExpressionUtils(){
  }

  /**
   * Get all series path of expression group by group id
   * @return
   */
  public static void getAllExpressionSeries(IExpression expression, Map<String, List<Path>> seriesPathMap)
      throws PathErrorException {
    if(expression.getType() == ExpressionType.SERIES){
      Path path = ((SingleSeriesExpression)expression).getSeriesPath();
      String storageGroup = QPExecutorUtils.getStroageGroupByDevice(path.getDevice());
      String groupId = Router.getInstance().getGroupIdBySG(storageGroup);
      if(!seriesPathMap.containsKey(groupId)){
        seriesPathMap.put(groupId, new ArrayList<>());
      }
      seriesPathMap.get(groupId).add(path);
    } else if (expression.getType() == OR || expression.getType() == AND) {
      getAllExpressionSeries(((IBinaryExpression)expression).getLeft(), seriesPathMap);
      getAllExpressionSeries(((IBinaryExpression)expression).getRight(), seriesPathMap);
    } else {
      throw new UnSupportedDataTypeException(
          "Unsupported QueryFilterType when construct OperatorNode: " + expression.getType());
    }
  }

  /**
   * Prune filter true by group id
   *
   * @param pathList all paths of a data group
   */
  public static IExpression pruneFilterTree(IExpression expression, List<Path> pathList) {
    if (expression.getType() == SERIES) {
      if (pathList.contains(((SingleSeriesExpression) expression).getSeriesPath())) {
        return expression;
      } else{
        return new TrueExpression();
      }
    } else if(expression.getType() == OR){
      return pruneOrFilterTree(expression, pathList);
    } else if(expression.getType() == AND){
      return pruneAndFilterTree(expression, pathList);
    } else {
      throw new UnSupportedDataTypeException(
          "Unsupported ExpressionType when prune filter tree: " + expression.getType());
    }
  }

  /**
   * Prune or filter tree
   *
   * @param expression origin expression
   * @param pathList all series path of the same data group
   */
  private static IExpression pruneOrFilterTree(IExpression expression, List<Path> pathList) {
    IExpression left = pruneFilterTree(((BinaryExpression) expression).getLeft(), pathList);
    IExpression right = pruneFilterTree(((BinaryExpression) expression).getRight(), pathList);
    if (left.getType() == TRUE || right.getType() == TRUE) {
      return new TrueExpression();
    } else {
      return expression;
    }
  }

  /**
   * Prune and filter tree
   *
   * @param expression origin expression
   * @param pathList all series path of the same data group
   */
  private static IExpression pruneAndFilterTree(IExpression expression, List<Path> pathList) {
    IExpression left = pruneFilterTree(((BinaryExpression) expression).getLeft(), pathList);
    IExpression right = pruneFilterTree(((BinaryExpression) expression).getRight(), pathList);
    if (left.getType() == TRUE && right.getType() == TRUE) {
      return new TrueExpression();
    } else if (left.getType() == TRUE) {
      return ((BinaryExpression) expression).getRight();
    } else if (right.getType() == TRUE) {
      return ((BinaryExpression) expression).getLeft();
    } else {
      return expression;
    }
  }

}
