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

package org.apache.iotdb.db.metadata.view.viewExpression.visitor;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.BinaryViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.leaf.TimeSeriesViewOperand;
import org.apache.iotdb.commons.schema.view.viewExpression.multi.FunctionViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.ternary.TernaryViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.unary.UnaryViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.visitor.ViewExpressionVisitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Use this visitor to find all the paths of time series used in one expression. */
public class GetSourcePathsVisitor extends ViewExpressionVisitor<List<PartialPath>, Void> {

  public static List<PartialPath> getSourcePaths(ViewExpression viewExpression) {
    return new GetSourcePathsVisitor().process(viewExpression, null);
  }

  @Override
  public List<PartialPath> visitExpression(ViewExpression expression, Void context) {
    return new ArrayList<>();
  }

  @Override
  public List<PartialPath> visitTimeSeriesOperand(
      TimeSeriesViewOperand timeSeriesOperand, Void context) {
    String pathString = timeSeriesOperand.getPathString();
    try {
      PartialPath path = new PartialPath(pathString);
      return Collections.singletonList(path);
    } catch (IllegalPathException e) {
      // the path is illegal!
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<PartialPath> visitUnaryExpression(
      UnaryViewExpression unaryViewExpression, Void context) {
    ViewExpression expression = unaryViewExpression.getExpression();
    return this.process(expression, null);
  }

  @Override
  public List<PartialPath> visitBinaryExpression(
      BinaryViewExpression binaryViewExpression, Void context) {
    List<PartialPath> result = new ArrayList<>();
    List<ViewExpression> expressionList = binaryViewExpression.getChildViewExpressions();
    for (ViewExpression expression : expressionList) {
      result.addAll(this.process(expression, null));
    }
    return result;
  }

  @Override
  public List<PartialPath> visitTernaryExpression(
      TernaryViewExpression ternaryViewExpression, Void context) {
    List<PartialPath> result = new ArrayList<>();
    List<ViewExpression> expressionList = ternaryViewExpression.getChildViewExpressions();
    for (ViewExpression expression : expressionList) {
      result.addAll(this.process(expression, null));
    }
    return result;
  }

  @Override
  public List<PartialPath> visitFunctionExpression(
      FunctionViewExpression functionViewExpression, Void context) {
    List<PartialPath> result = new ArrayList<>();
    List<ViewExpression> expressionList = functionViewExpression.getChildViewExpressions();
    for (ViewExpression expression : expressionList) {
      result.addAll(this.process(expression, null));
    }
    return result;
  }
}
