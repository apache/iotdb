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

package org.apache.iotdb.db.queryengine.plan.execution.memory;

import org.apache.iotdb.commons.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Table;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.tsfile.utils.Pair;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class TableModelStatementMemorySourceVisitorTest {

  @Test
  public void testMergeExplainResultsJsonReturnsMainResultWithoutCte() throws Exception {
    List<String> mainExplainResult =
        Collections.singletonList("{\"name\":\"OutputNode-main\",\"id\":\"main\"}");

    List<String> result = mergeExplainResultsJson(Collections.emptyMap(), mainExplainResult);

    assertSame(mainExplainResult, result);
  }

  @Test
  public void testMergeExplainResultsJsonWrapsCteAndMainPlans() throws Exception {
    Map<NodeRef<Table>, Pair<Integer, List<String>>> cteExplainResults = new LinkedHashMap<>();
    cteExplainResults.put(
        NodeRef.of(new Table(QualifiedName.of("cte1"))),
        new Pair<>(0, Collections.singletonList("{\"name\":\"OutputNode-cte\",\"id\":\"cte\"}")));
    List<String> mainExplainResult =
        Collections.singletonList("{\"name\":\"OutputNode-main\",\"id\":\"main\"}");

    List<String> result = mergeExplainResultsJson(cteExplainResults, mainExplainResult);

    assertEquals(1, result.size());
    JsonObject root = JsonParser.parseString(result.get(0)).getAsJsonObject();
    JsonArray cteQueries = root.getAsJsonArray("cteQueries");
    assertEquals(1, cteQueries.size());

    JsonObject cte = cteQueries.get(0).getAsJsonObject();
    assertEquals("cte1", cte.get("name").getAsString());
    assertEquals("OutputNode-cte", cte.getAsJsonObject("plan").get("name").getAsString());
    assertEquals("OutputNode-main", root.getAsJsonObject("mainQuery").get("name").getAsString());
  }

  @SuppressWarnings("unchecked")
  private static List<String> mergeExplainResultsJson(
      Map<NodeRef<Table>, Pair<Integer, List<String>>> cteExplainResults,
      List<String> mainExplainResult)
      throws Exception {
    Method method =
        TableModelStatementMemorySourceVisitor.class.getDeclaredMethod(
            "mergeExplainResultsJson", Map.class, List.class);
    method.setAccessible(true);
    return (List<String>)
        method.invoke(
            new TableModelStatementMemorySourceVisitor(), cteExplainResults, mainExplainResult);
  }
}
