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

package org.apache.iotdb.db.queryengine.udf;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.commons.queryengine.plan.udf.TableUDFUtils;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DefaultTraversalVisitor;

import java.util.Optional;

/** Detects whether filter or project expressions contain a user-defined scalar function. */
public final class ScalarUdfExpressionDetector extends DefaultTraversalVisitor<Void> {

  private boolean found;

  private ScalarUdfExpressionDetector() {}

  public static boolean contains(Optional<Expression> predicate, Expression[] projectExpressions) {
    ScalarUdfExpressionDetector detector = new ScalarUdfExpressionDetector();
    if (predicate.isPresent()) {
      detector.process(predicate.get(), null);
    }
    for (Expression expression : projectExpressions) {
      if (detector.found) {
        return true;
      }
      detector.process(expression, null);
    }
    return detector.found;
  }

  @Override
  public Void visitFunctionCall(FunctionCall node, Void context) {
    if (TableUDFUtils.isScalarFunction(node.getName().getSuffix())) {
      found = true;
      return null;
    }
    return super.visitFunctionCall(node, context);
  }
}
