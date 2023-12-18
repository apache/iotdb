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
package org.apache.iotdb.tsfile.read.filter;

import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.expression.util.ExpressionOptimizer;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.iotdb.tsfile.read.filter.factory.ValueFilterApi;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class IExpressionOptimizerTest {

  private final ExpressionOptimizer expressionOptimizer = ExpressionOptimizer.getInstance();
  private List<Path> selectedSeries;

  @Before
  public void before() {
    selectedSeries = new ArrayList<>();
    selectedSeries.add(new Path("d1", "s1", true));
    selectedSeries.add(new Path("d2", "s1", true));
    selectedSeries.add(new Path("d1", "s2", true));
    selectedSeries.add(new Path("d2", "s2", true));
  }

  @After
  public void after() {
    selectedSeries.clear();
  }

  @Test
  public void testTimeOnly() {
    try {
      Filter timeFilter = TimeFilterApi.lt(100L);
      IExpression expression = new GlobalTimeExpression(timeFilter);
      System.out.println(expressionOptimizer.optimize(expression, selectedSeries));

      BinaryExpression.or(
          BinaryExpression.and(
              new GlobalTimeExpression(TimeFilterApi.lt(50L)),
              new GlobalTimeExpression(TimeFilterApi.gt(10L))),
          new GlobalTimeExpression(TimeFilterApi.gt(200L)));

    } catch (QueryFilterOptimizationException e) {
      Assert.fail();
    }
  }

  @Test
  public void testSeriesOnly() {
    try {
      Filter filter1 =
          FilterFactory.and(
              FilterFactory.or(ValueFilterApi.gt(100L), ValueFilterApi.lt(50L)),
              TimeFilterApi.gt(1400L));
      SingleSeriesExpression singleSeriesExp1 =
          new SingleSeriesExpression(new Path("d2", "s1", true), filter1);

      Filter filter2 =
          FilterFactory.and(
              FilterFactory.or(ValueFilterApi.gt(100.5f), ValueFilterApi.lt(50.6f)),
              TimeFilterApi.gt(1400L));
      SingleSeriesExpression singleSeriesExp2 =
          new SingleSeriesExpression(new Path("d1", "s2", true), filter2);

      Filter filter3 =
          FilterFactory.or(
              FilterFactory.or(ValueFilterApi.gt(100.5), ValueFilterApi.lt(50.6)),
              TimeFilterApi.gt(1400L));
      SingleSeriesExpression singleSeriesExp3 =
          new SingleSeriesExpression(new Path("d2", "s2", true), filter3);

      IExpression expression =
          BinaryExpression.and(
              BinaryExpression.or(singleSeriesExp1, singleSeriesExp2), singleSeriesExp3);
      Assert.assertEquals(
          expression.toString(),
          expressionOptimizer.optimize(expression, selectedSeries).toString());

    } catch (QueryFilterOptimizationException e) {
      Assert.fail();
    }
  }

  @Test
  public void testOneTimeAndSeries() {
    Filter filter1 = FilterFactory.or(ValueFilterApi.gt(100L), ValueFilterApi.lt(50L));
    SingleSeriesExpression singleSeriesExp1 =
        new SingleSeriesExpression(new Path("d2", "s1", true), filter1);

    Filter filter2 = FilterFactory.or(ValueFilterApi.gt(100.5f), ValueFilterApi.lt(50.6f));
    SingleSeriesExpression singleSeriesExp2 =
        new SingleSeriesExpression(new Path("d1", "s2", true), filter2);

    Filter timeFilter = TimeFilterApi.lt(14001234L);
    IExpression globalTimeFilter = new GlobalTimeExpression(timeFilter);
    IExpression expression =
        BinaryExpression.and(
            BinaryExpression.or(singleSeriesExp1, singleSeriesExp2), globalTimeFilter);
    try {
      String rightRet =
          "[[d2.s1:((measurements[0] > 100 || measurements[0] < 50) && time < 14001234)] "
              + "|| [d1.s2:((measurements[0] > 100.5 || measurements[0] < 50.6) "
              + "&& time < 14001234)]]";
      IExpression regularFilter = expressionOptimizer.optimize(expression, selectedSeries);
      Assert.assertEquals(rightRet, regularFilter.toString());
    } catch (QueryFilterOptimizationException e) {
      Assert.fail();
    }
  }

  @Test
  public void testSeriesAndGlobalOrGlobal() {
    Filter filter1 = FilterFactory.or(ValueFilterApi.gt(100L), ValueFilterApi.lt(50L));
    SingleSeriesExpression singleSeriesExp1 =
        new SingleSeriesExpression(new Path("d2", "s1", true), filter1);

    Filter timeFilter = TimeFilterApi.lt(14001234L);
    IExpression globalTimeFilter = new GlobalTimeExpression(timeFilter);

    Filter timeFilter2 = TimeFilterApi.gt(1L);
    IExpression globalTimeFilter2 = new GlobalTimeExpression(timeFilter2);

    IExpression expression =
        BinaryExpression.or(
            BinaryExpression.and(singleSeriesExp1, globalTimeFilter), globalTimeFilter2);
    try {
      String rightRet =
          "[[[[d1.s1:time > 1] || "
              + "[d2.s1:(time > 1 || ((measurements[0] > 100 || measurements[0] < 50) "
              + "&& time < 14001234))]] || [d1.s2:time > 1]] || [d2.s2:time > 1]]";
      IExpression regularFilter = expressionOptimizer.optimize(expression, selectedSeries);
      Assert.assertEquals(rightRet, regularFilter.toString());
    } catch (QueryFilterOptimizationException e) {
      Assert.fail();
    }
  }

  @Test
  public void testSeriesAndGlobal() {
    Filter filter1 = FilterFactory.or(ValueFilterApi.gt(100L), ValueFilterApi.lt(50L));
    SingleSeriesExpression singleSeriesExp1 =
        new SingleSeriesExpression(new Path("d2", "s1", true), filter1);

    Filter timeFilter = TimeFilterApi.lt(14001234L);
    IExpression globalTimeFilter = new GlobalTimeExpression(timeFilter);

    IExpression expression = BinaryExpression.and(singleSeriesExp1, globalTimeFilter);

    try {
      String rightRet =
          "[d2.s1:((measurements[0] > 100 || measurements[0] < 50) && time < 14001234)]";
      IExpression regularFilter = expressionOptimizer.optimize(expression, selectedSeries);
      Assert.assertEquals(rightRet, regularFilter.toString());
    } catch (QueryFilterOptimizationException e) {
      Assert.fail();
    }
  }

  @Test
  public void testOneTimeOrSeries() {
    Filter filter1 = FilterFactory.or(ValueFilterApi.gt(100L), ValueFilterApi.lt(50L));
    SingleSeriesExpression singleSeriesExp1 =
        new SingleSeriesExpression(new Path("d2", "s1", true), filter1);

    Filter filter2 = FilterFactory.or(ValueFilterApi.gt(100.5f), ValueFilterApi.lt(50.6f));
    SingleSeriesExpression singleSeriesExp2 =
        new SingleSeriesExpression(new Path("d1", "s2", true), filter2);

    Filter timeFilter = TimeFilterApi.lt(14001234L);
    IExpression globalTimeFilter = new GlobalTimeExpression(timeFilter);
    IExpression expression =
        BinaryExpression.or(
            BinaryExpression.or(singleSeriesExp1, singleSeriesExp2), globalTimeFilter);

    try {
      String rightRet =
          "[[[[d1.s1:time < 14001234] "
              + "|| [d2.s1:(time < 14001234 || "
              + "(measurements[0] > 100 || measurements[0] < 50))]] "
              + "|| [d1.s2:(time < 14001234 || "
              + "(measurements[0] > 100.5 || measurements[0] < 50.6))]] "
              + "|| [d2.s2:time < 14001234]]";
      IExpression regularFilter = expressionOptimizer.optimize(expression, selectedSeries);
      Assert.assertEquals(rightRet, regularFilter.toString());
    } catch (QueryFilterOptimizationException e) {
      Assert.fail();
    }
  }

  @Test
  public void testTwoTimeCombine() {
    Filter filter1 = FilterFactory.or(ValueFilterApi.gt(100L), ValueFilterApi.lt(50L));
    SingleSeriesExpression singleSeriesExp1 =
        new SingleSeriesExpression(new Path("d2", "s1", true), filter1);

    Filter filter2 = FilterFactory.or(ValueFilterApi.gt(100.5f), ValueFilterApi.lt(50.6f));
    SingleSeriesExpression singleSeriesExp2 =
        new SingleSeriesExpression(new Path("d1", "s2", true), filter2);

    IExpression globalTimeFilter1 = new GlobalTimeExpression(TimeFilterApi.lt(14001234L));
    IExpression globalTimeFilter2 = new GlobalTimeExpression(TimeFilterApi.gt(14001000L));
    IExpression expression =
        BinaryExpression.or(
            BinaryExpression.or(singleSeriesExp1, singleSeriesExp2),
            BinaryExpression.and(globalTimeFilter1, globalTimeFilter2));

    try {
      String rightRet =
          "[[[[d1.s1:(time < 14001234 && time > 14001000)] "
              + "|| [d2.s1:((time < 14001234 && time > 14001000) "
              + "|| (measurements[0] > 100 || measurements[0] < 50))]] "
              + "|| [d1.s2:((time < 14001234 && time > 14001000) "
              + "|| (measurements[0] > 100.5 || measurements[0] < 50.6))]] "
              + "|| [d2.s2:(time < 14001234 && time > 14001000)]]";
      IExpression regularFilter = expressionOptimizer.optimize(expression, selectedSeries);
      Assert.assertEquals(rightRet, regularFilter.toString());
    } catch (QueryFilterOptimizationException e) {
      Assert.fail();
    }

    IExpression expression2 =
        BinaryExpression.and(
            BinaryExpression.or(singleSeriesExp1, singleSeriesExp2),
            BinaryExpression.and(globalTimeFilter1, globalTimeFilter2));

    try {
      String rightRet2 =
          "[[d2.s1:((measurements[0] > 100 || measurements[0] < 50) "
              + "&& (time < 14001234 && time > 14001000))] || "
              + "[d1.s2:((measurements[0] > 100.5 || measurements[0] < 50.6) "
              + "&& (time < 14001234 && time > 14001000))]]";
      IExpression regularFilter2 = expressionOptimizer.optimize(expression2, selectedSeries);
      Assert.assertEquals(rightRet2, regularFilter2.toString());
    } catch (QueryFilterOptimizationException e) {
      Assert.fail();
    }

    IExpression expression3 = BinaryExpression.or(expression2, expression);
    try {
      expressionOptimizer.optimize(expression3, selectedSeries);
    } catch (QueryFilterOptimizationException e) {
      Assert.fail();
    }
  }
}
