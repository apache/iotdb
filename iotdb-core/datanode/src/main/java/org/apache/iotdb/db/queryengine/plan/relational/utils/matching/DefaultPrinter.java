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
package org.apache.iotdb.db.queryengine.plan.relational.utils.matching;

import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.pattern.CapturePattern;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.pattern.EqualsPattern;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.pattern.FilterPattern;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.pattern.TypeOfPattern;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.pattern.WithPattern;

import java.util.Collections;

import static java.lang.String.format;

public class DefaultPrinter implements PatternVisitor {
  private final StringBuilder result = new StringBuilder();
  private int level;

  public String result() {
    return result.toString();
  }

  @Override
  public void visitTypeOf(TypeOfPattern<?> pattern) {
    visitPrevious(pattern);
    appendLine("typeOf(%s)", pattern.expectedClass().getSimpleName());
  }

  @Override
  public void visitWith(WithPattern<?> pattern) {
    visitPrevious(pattern);
    appendLine("with(%s)", pattern.getProperty().getName());
    level += 1;
    pattern.getPattern().accept(this);
    level -= 1;
  }

  @Override
  public void visitCapture(CapturePattern<?> pattern) {
    visitPrevious(pattern);
    appendLine("capturedAs(%s)", pattern.capture().description());
  }

  @Override
  public void visitEquals(EqualsPattern<?> pattern) {
    visitPrevious(pattern);
    appendLine("equals(%s)", pattern.expectedValue());
  }

  @Override
  public void visitFilter(FilterPattern<?> pattern) {
    visitPrevious(pattern);
    appendLine("filter(%s)", pattern.predicate());
  }

  private void appendLine(String template, Object... arguments) {
    result
        .append(String.join("", Collections.nCopies(level, "\t")))
        .append(format(template + "\n", arguments));
  }
}
