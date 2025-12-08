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
package org.apache.iotdb.db.queryengine.plan.relational.sql;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DefaultTraversalVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Parameter;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;

import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;

/** Utility class for extracting and binding parameters in prepared statements. */
public final class ParameterExtractor {
  private ParameterExtractor() {}

  /**
   * Get the number of parameters in a statement.
   *
   * @param statement the statement to analyze
   * @return the number of parameters
   */
  public static int getParameterCount(Statement statement) {
    return extractParameters(statement).size();
  }

  /**
   * Extract all Parameter nodes from a statement in order of appearance.
   *
   * @param statement the statement to analyze
   * @return list of Parameter nodes in order of appearance
   */
  public static List<Parameter> extractParameters(Statement statement) {
    ParameterExtractingVisitor visitor = new ParameterExtractingVisitor();
    visitor.process(statement, null);
    return visitor.getParameters().stream()
        .sorted(
            Comparator.comparing(
                parameter ->
                    parameter
                        .getLocation()
                        .orElseThrow(
                            () -> new SemanticException("Parameter node must have a location")),
                Comparator.comparing(
                        org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NodeLocation
                            ::getLineNumber)
                    .thenComparing(
                        org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NodeLocation
                            ::getColumnNumber)))
        .collect(toImmutableList());
  }

  /**
   * Bind parameter values to Parameter nodes in a statement. Creates a map from Parameter node
   * references to their corresponding Expression values.
   *
   * @param statement the statement containing Parameter nodes
   * @param values the parameter values (in order)
   * @return map from Parameter node references to Expression values
   * @throws SemanticException if the number of parameters doesn't match
   */
  public static Map<NodeRef<Parameter>, Expression> bindParameters(
      Statement statement, List<Literal> values) {
    List<Parameter> parametersList = extractParameters(statement);

    // Validate parameter count
    if (parametersList.size() != values.size()) {
      throw new SemanticException(
          String.format(
              "Invalid number of parameters: expected %d, got %d",
              parametersList.size(), values.size()));
    }

    ImmutableMap.Builder<NodeRef<Parameter>, Expression> builder = ImmutableMap.builder();
    Iterator<Literal> iterator = values.iterator();
    for (Parameter parameter : parametersList) {
      builder.put(NodeRef.of(parameter), iterator.next());
    }
    return builder.buildOrThrow();
  }

  private static class ParameterExtractingVisitor extends DefaultTraversalVisitor<Void> {
    private final List<Parameter> parameters = new ArrayList<>();

    public List<Parameter> getParameters() {
      return parameters;
    }

    @Override
    protected Void visitParameter(Parameter node, Void context) {
      parameters.add(node);
      return null;
    }
  }
}
