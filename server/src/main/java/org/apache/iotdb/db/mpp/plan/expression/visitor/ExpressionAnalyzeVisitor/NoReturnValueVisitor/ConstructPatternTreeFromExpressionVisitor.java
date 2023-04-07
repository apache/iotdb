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

package org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionAnalyzeVisitor.NoReturnValueVisitor;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.constant.SqlConstant;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import java.util.List;

public class ConstructPatternTreeFromExpressionVisitor
    extends NoReturnValueVisitor<ConstructPatternTreeFromExpressionVisitor.Context> {
  @Override
  public Void visitTimeSeriesOperand(TimeSeriesOperand timeSeriesOperand, Context context) {
    PartialPath rawPath = timeSeriesOperand.getPath();
    if (rawPath.getFullPath().startsWith(SqlConstant.ROOT + TsFileConstant.PATH_SEPARATOR)) {
      context.patternTree.appendPathPattern(rawPath);
      return null;
    }
    for (PartialPath prefixPath : context.prefixPaths) {
      PartialPath concatPath = prefixPath.concatPath(rawPath);
      context.patternTree.appendPathPattern(concatPath);
    }
    return null;
  }

  public static class Context {
    List<PartialPath> prefixPaths;
    PathPatternTree patternTree;

    public Context(List<PartialPath> prefixPaths, PathPatternTree patternTree) {
      this.prefixPaths = prefixPaths;
      this.patternTree = patternTree;
    }
  }
}
