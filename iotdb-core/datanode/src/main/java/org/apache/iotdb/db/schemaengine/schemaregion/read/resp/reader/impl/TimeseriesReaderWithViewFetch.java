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

package org.apache.iotdb.db.schemaengine.schemaregion.read.resp.reader.impl;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.SchemaFilterType;
import org.apache.iotdb.commons.schema.tree.SchemaIterator;
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.queryengine.common.NodeRef;
import org.apache.iotdb.db.queryengine.common.schematree.ISchemaTree;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceManager;
import org.apache.iotdb.db.queryengine.plan.analyze.ExpressionTypeAnalyzer;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.visitor.CompleteMeasurementSchemaVisitor;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.ITimeSeriesSchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.reader.ISchemaReader;
import org.apache.iotdb.db.schemaengine.schemaregion.utils.filter.FilterContainsVisitor;
import org.apache.iotdb.db.schemaengine.schemaregion.utils.filter.TimeseriesFilterVisitor;
import org.apache.iotdb.db.schemaengine.schemaregion.view.visitor.GetSourcePathsVisitor;
import org.apache.iotdb.db.schemaengine.schemaregion.view.visitor.TransformToExpressionVisitor;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;

public class TimeseriesReaderWithViewFetch implements ISchemaReader<ITimeSeriesSchemaInfo> {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimeseriesReaderWithViewFetch.class);
  private final SchemaIterator<ITimeSeriesSchemaInfo> iterator;
  private final Queue<ITimeSeriesSchemaInfo> cachedViewList = new ArrayDeque<>();
  private ITimeSeriesSchemaInfo next = null;
  private boolean consumeView = false;
  private final SchemaFilter schemaFilter;

  /**
   * There is no need to pull the original sequence information from the view if needFetch is false.
   * The default is false if not filtered by DataType.
   */
  private final boolean needFetch;

  /**
   * If isBlocked is null, it means the next is not fetched yet. If isBlocked.isDone() is false, it
   * means the next is being fetched. If isBlocked.get() is true, it means hasNext, otherwise, it
   * means no more info to be fetched.
   */
  private ListenableFuture<Boolean> isBlocked = null;

  private static final int BATCH_CACHED_SIZE = 1000;
  private static final TimeseriesFilterVisitor FILTER_VISITOR = new TimeseriesFilterVisitor();

  public TimeseriesReaderWithViewFetch(
      SchemaIterator<ITimeSeriesSchemaInfo> iterator, SchemaFilter schemaFilter) {
    this.iterator = iterator;
    this.schemaFilter = schemaFilter;
    this.needFetch = new FilterContainsVisitor().process(schemaFilter, SchemaFilterType.DATA_TYPE);
  }

  public TimeseriesReaderWithViewFetch(
      SchemaIterator<ITimeSeriesSchemaInfo> iterator,
      SchemaFilter schemaFilter,
      boolean needViewDetail) {
    this.iterator = iterator;
    this.schemaFilter = schemaFilter;
    this.needFetch =
        needViewDetail
            || new FilterContainsVisitor().process(schemaFilter, SchemaFilterType.DATA_TYPE);
  }

  @Override
  public boolean isSuccess() {
    return iterator.isSuccess();
  }

  @Override
  public Throwable getFailure() {
    return iterator.getFailure();
  }

  @Override
  public void close() {
    iterator.close();
  }

  /**
   * Fetch ITimeSeriesSchemaInfo from the iterator and return only in the following three cases
   *
   * <ol>
   *   <li>successfully fetched an info of normal time series. consumeView is false and next is not
   *       null.
   *   <li>successfully fetched batch info of view time series. consumeView is true and next is
   *       null.
   *   <li>no more info to be fetched. consumeView is false and next is null.
   * </ol>
   */
  @Override
  public ListenableFuture<Boolean> isBlocked() {
    if (isBlocked != null) {
      return isBlocked;
    }
    ListenableFuture<Boolean> res = NOT_BLOCKED_FALSE;
    if (consumeView) {
      // consume view list
      res = NOT_BLOCKED_TRUE;
    } else if (next == null) {
      // get next from iterator
      ITimeSeriesSchemaInfo temp;
      while (iterator.hasNext()) {
        temp = iterator.next();
        if (needFetch && temp.isLogicalView()) {
          // view timeseries
          cachedViewList.add(temp.snapshot());
          if (cachedViewList.size() >= BATCH_CACHED_SIZE) {
            res = asyncGetNext();
            break;
          }
        } else if (FILTER_VISITOR.process(schemaFilter, temp)) {
          // normal timeseries
          next = temp;
          res = NOT_BLOCKED_TRUE;
          break;
        }
      }
      if (res == NOT_BLOCKED_FALSE && !cachedViewList.isEmpty()) {
        // all schema info has been fetched, but there mau be still some view schema info in
        // cachedViewList
        res = asyncGetNext();
      }
    } else {
      // next is not null
      res = NOT_BLOCKED_TRUE;
    }
    isBlocked = res;
    return res;
  }

  @SuppressWarnings("java:S2142")
  @Override
  public boolean hasNext() {
    try {
      return isBlocked().get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ITimeSeriesSchemaInfo next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    ITimeSeriesSchemaInfo result;
    if (!consumeView) {
      result = next;
      next = null;
    } else {
      // it may return null if cachedViewList is empty but consumeView is true
      result = cachedViewList.poll();
      consumeView = !cachedViewList.isEmpty();
    }
    isBlocked = null;
    return result;
  }

  private ListenableFuture<Boolean> asyncGetNext() {
    // enter this function only when viewList is full or all schema info has been fetched and
    // viewList is not empty
    return Futures.submit(
        () -> {
          fetchViewTimeSeriesSchemaInfo();
          if (consumeView) {
            return true;
          } else {
            // all cache view is no satisfied
            while (iterator.hasNext()) {
              ITimeSeriesSchemaInfo temp = iterator.next();
              if (temp.isLogicalView()) {
                cachedViewList.add(temp.snapshot());
                if (cachedViewList.size() >= BATCH_CACHED_SIZE) {
                  fetchViewTimeSeriesSchemaInfo();
                  if (consumeView) {
                    return true;
                  }
                }
              } else if (FILTER_VISITOR.process(schemaFilter, temp)) {
                next = temp;
                return true;
              }
            }
            return false;
          }
        },
        FragmentInstanceManager.getInstance().getIntoOperationExecutor());
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

    ISchemaTree schemaTree =
        ClusterSchemaFetcher.getInstance().fetchSchema(patternTree, true, null);
    // process each view expression and get data type
    TransformToExpressionVisitor transformToExpressionVisitor = new TransformToExpressionVisitor();
    CompleteMeasurementSchemaVisitor completeMeasurementSchemaVisitor =
        new CompleteMeasurementSchemaVisitor();
    Map<NodeRef<Expression>, TSDataType> expressionTypes = new HashMap<>();
    for (int i = 0; i < delayedLogicalViewList.size(); i++) {
      ViewExpression viewExpression = viewExpressionList.get(i);
      Expression expression = null;
      boolean viewIsBroken = false;
      try {
        expression = transformToExpressionVisitor.process(viewExpression, null);
        expression = completeMeasurementSchemaVisitor.process(expression, schemaTree);
        ExpressionTypeAnalyzer.analyzeExpression(expressionTypes, expression);
      } catch (Exception e) {
        viewIsBroken = true;
      }
      delayedLogicalViewList
          .get(i)
          .getSchema()
          .setDataType(
              viewIsBroken ? TSDataType.UNKNOWN : expressionTypes.get(NodeRef.of(expression)));
      if (FILTER_VISITOR.process(schemaFilter, delayedLogicalViewList.get(i))) {
        cachedViewList.add(delayedLogicalViewList.get(i));
      }
    }
    consumeView = !cachedViewList.isEmpty();
  }
}
