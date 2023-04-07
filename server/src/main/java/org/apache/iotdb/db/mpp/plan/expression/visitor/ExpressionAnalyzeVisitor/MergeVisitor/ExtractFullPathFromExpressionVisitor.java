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

package org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionAnalyzeVisitor.MergeVisitor;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.constant.SqlConstant;
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.NullOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ExtractFullPathFromExpressionVisitor
    extends MergeVisitor<List<PartialPath>, List<PartialPath>> {
  @Override
  List<PartialPath> merge(List<List<PartialPath>> childResults) {
    List<PartialPath> result = new ArrayList<>();
    childResults.forEach(result::addAll);
    return result;
  }

  @Override
  public List<PartialPath> visitTimeSeriesOperand(
      TimeSeriesOperand timeSeriesOperand, List<PartialPath> context) {
    PartialPath rawPath = timeSeriesOperand.getPath();
    List<PartialPath> actualPaths = new ArrayList<>();
    if (rawPath.getFullPath().startsWith(SqlConstant.ROOT + TsFileConstant.PATH_SEPARATOR)) {
      actualPaths.add(rawPath);
    } else {
      for (PartialPath prefixPath : context) {
        PartialPath concatPath = prefixPath.concatPath(rawPath);
        actualPaths.add(concatPath);
      }
    }
    return actualPaths;
  }

  @Override
  public List<PartialPath> visitTimeStampOperand(
      TimestampOperand timestampOperand, List<PartialPath> context) {
    return Collections.emptyList();
  }

  @Override
  public List<PartialPath> visitConstantOperand(
      ConstantOperand constantOperand, List<PartialPath> context) {
    return Collections.emptyList();
  }

  @Override
  public List<PartialPath> visitNullOperand(NullOperand nullOperand, List<PartialPath> context) {
    return Collections.emptyList();
  }
}
