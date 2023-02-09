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

package org.apache.iotdb.db.mpp.plan.analyze;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.mpp.common.schematree.ISchemaTree;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.mpp.plan.expression.ExpressionFactory.and;
import static org.apache.iotdb.db.mpp.plan.expression.ExpressionFactory.count;
import static org.apache.iotdb.db.mpp.plan.expression.ExpressionFactory.gt;
import static org.apache.iotdb.db.mpp.plan.expression.ExpressionFactory.intValue;
import static org.apache.iotdb.db.mpp.plan.expression.ExpressionFactory.path;
import static org.apache.iotdb.db.mpp.plan.expression.ExpressionFactory.timeSeries;
import static org.junit.Assert.assertEquals;

public class ExpressionAnalyzerTest {

  @Test
  public void testRemoveWildcardInFilter() throws IllegalPathException {
    ISchemaTree fakeSchemaTree = new FakeSchemaFetcherImpl().fetchSchema(new PathPatternTree());
    List<PartialPath> prefixPaths = Arrays.asList(path("root.sg.d1"), path("root.sg.d2"));

    assertEquals(
        Arrays.asList(
            gt(timeSeries("root.sg.d1.s1"), intValue("1")),
            gt(timeSeries("root.sg.d2.s1"), intValue("1")),
            gt(timeSeries("root.sg.d1.s2"), intValue("1")),
            gt(timeSeries("root.sg.d2.s2"), intValue("1"))),
        ExpressionAnalyzer.removeWildcardInFilter(
            and(gt(timeSeries("s1"), intValue("1")), gt(timeSeries("s2"), intValue("1"))),
            prefixPaths,
            fakeSchemaTree,
            true));

    assertEquals(
        Arrays.asList(
            count(
                and(
                    gt(timeSeries("root.sg.d1.s1"), intValue("1")),
                    gt(timeSeries("root.sg.d1.s2"), intValue("1")))),
            count(
                and(
                    gt(timeSeries("root.sg.d1.s1"), intValue("1")),
                    gt(timeSeries("root.sg.d2.s2"), intValue("1")))),
            count(
                and(
                    gt(timeSeries("root.sg.d2.s1"), intValue("1")),
                    gt(timeSeries("root.sg.d1.s2"), intValue("1")))),
            count(
                and(
                    gt(timeSeries("root.sg.d2.s1"), intValue("1")),
                    gt(timeSeries("root.sg.d2.s2"), intValue("1"))))),
        ExpressionAnalyzer.removeWildcardInFilter(
            count(and(gt(timeSeries("s1"), intValue("1")), gt(timeSeries("s2"), intValue("1")))),
            prefixPaths,
            fakeSchemaTree,
            true));
  }
}
