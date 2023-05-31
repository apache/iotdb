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
package org.apache.iotdb.db.metadata.query.reader;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.metadata.mtree.traverser.Traverser;
import org.apache.iotdb.db.metadata.query.info.ITimeSeriesSchemaInfo;
import org.apache.iotdb.db.metadata.view.viewExpression.visitor.GetSourcePathsVisitor;
import org.apache.iotdb.db.metadata.view.viewExpression.visitor.TransformToExpressionVisitor;
import org.apache.iotdb.db.metadata.visitor.TimeseriesFilterVisitor;
import org.apache.iotdb.db.mpp.common.NodeRef;
import org.apache.iotdb.db.mpp.common.schematree.ISchemaTree;
import org.apache.iotdb.db.mpp.plan.analyze.ExpressionTypeAnalyzer;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.visitor.CompleteMeasurementSchemaVisitor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;

public class TimeseriesReaderWithViewFetch implements ISchemaReader<ITimeSeriesSchemaInfo> {

  private final Traverser<ITimeSeriesSchemaInfo, ?> traverser;
  private final Queue<ITimeSeriesSchemaInfo> cachedViewList = new ArrayDeque<>();
  private ITimeSeriesSchemaInfo next = null;
  private boolean consumeView = false;
  private final SchemaFilter schemaFilter;

  private static final int BATCH_CACHED_SIZE = 1000;
  private static final TimeseriesFilterVisitor FILTER_VISITOR = new TimeseriesFilterVisitor();

  public TimeseriesReaderWithViewFetch(
      Traverser<ITimeSeriesSchemaInfo, ?> traverser, SchemaFilter schemaFilter) {
    this.traverser = traverser;
    this.schemaFilter = schemaFilter;
  }

  public boolean isSuccess() {
    return traverser.isSuccess();
  }

  public Throwable getFailure() {
    return traverser.getFailure();
  }

  public void close() {
    traverser.close();
  }

  public boolean hasNext() {
    if (next == null && !consumeView) {
      fetchAndCacheNextResult();
    }
    if (consumeView) {
      return !cachedViewList.isEmpty();
    } else {
      return next != null;
    }
  }

  public ITimeSeriesSchemaInfo next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    ITimeSeriesSchemaInfo result;
    if (!consumeView) {
      result = next;
      next = null;
    } else {
      result = cachedViewList.poll();
      consumeView = !cachedViewList.isEmpty();
    }
    return result;
  }

  /**
   * Fetch ITimeSeriesSchemaInfo from the traverser and return only in the following three cases
   *
   * <ol>
   *   <li>successfully fetched an info of normal time series. consumeView is false and next is not
   *       null.
   *   <li>successfully fetched batch info of view time series. consumeView is true and next is
   *       null.
   *   <li>no more info to be fetched. consumeView is false and next is null.
   * </ol>
   */
  private void fetchAndCacheNextResult() {
    ITimeSeriesSchemaInfo temp;
    while (traverser.hasNext()) {
      temp = traverser.next();
      if (temp.isLogicalView()) {
        cachedViewList.add(temp.snapshot());
        if (cachedViewList.size() >= BATCH_CACHED_SIZE) {
          fetchViewTimeSeriesSchemaInfo();
          if (consumeView) {
            break;
          }
        }
      } else {
        if (FILTER_VISITOR.process(schemaFilter, temp)) {
          next = temp;
          break;
        }
      }
    }
    if (next == null && !cachedViewList.isEmpty()) {
      // all schema info has been fetched, but there mau be still some view schema info in
      // cachedViewList
      fetchViewTimeSeriesSchemaInfo();
    }
  }

  private void fetchViewTimeSeriesSchemaInfo() {
    List<ITimeSeriesSchemaInfo> delayedLogicalViewList = new ArrayList<>();
    List<ViewExpression> viewExpressionList = new ArrayList<>();

    GetSourcePathsVisitor getSourcePathsVisitor = new GetSourcePathsVisitor();
    List<PartialPath> sourcePathsNeedFetch;
    PathPatternTree patternTree = new PathPatternTree();
    for (ITimeSeriesSchemaInfo series : cachedViewList) {
      delayedLogicalViewList.add(series);
      ViewExpression viewExpression = ((LogicalViewSchema) series.getSchema()).getExpression();
      viewExpressionList.add(((LogicalViewSchema) series.getSchema()).getExpression());
      sourcePathsNeedFetch = getSourcePathsVisitor.process(viewExpression, null);
      for (PartialPath path : sourcePathsNeedFetch) {
        patternTree.appendFullPath(path);
      }
    }
    // clear cachedViewList, all cached view will be added in the last step
    cachedViewList.clear();
    ISchemaTree schemaTree = ClusterSchemaFetcher.getInstance().fetchSchema(patternTree, null);
    // process each view expression and get data type
    TransformToExpressionVisitor transformToExpressionVisitor = new TransformToExpressionVisitor();
    CompleteMeasurementSchemaVisitor completeMeasurementSchemaVisitor =
        new CompleteMeasurementSchemaVisitor();
    Map<NodeRef<Expression>, TSDataType> expressionTypes = new HashMap<>();
    for (int i = 0; i < delayedLogicalViewList.size(); i++) {
      ViewExpression viewExpression = viewExpressionList.get(i);
      Expression expression = transformToExpressionVisitor.process(viewExpression, null);
      expression = completeMeasurementSchemaVisitor.process(expression, schemaTree);
      ExpressionTypeAnalyzer.analyzeExpression(expressionTypes, expression);
      delayedLogicalViewList
          .get(i)
          .getSchema()
          .setType(expressionTypes.get(NodeRef.of(expression)));
      if (FILTER_VISITOR.process(schemaFilter, delayedLogicalViewList.get(i))) {
        cachedViewList.add(delayedLogicalViewList.get(i));
      }
    }
    if (!cachedViewList.isEmpty()) {
      consumeView = true;
    }
  }
}
