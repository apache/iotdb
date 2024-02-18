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

package org.apache.iotdb.db.relational.sql.util;

import org.apache.iotdb.db.relational.sql.tree.AstVisitor;
import org.apache.iotdb.db.relational.sql.tree.Expression;
import org.apache.iotdb.db.relational.sql.tree.Literal;
import org.apache.iotdb.db.relational.sql.tree.SymbolReference;

import java.util.Optional;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public final class ExpressionFormatter {

  private ExpressionFormatter() {}

  public static String formatExpression(Expression expression) {
    return new Formatter(Optional.empty(), Optional.empty()).process(expression, null);
  }

  public static class Formatter extends AstVisitor<String, Void> {
    private final Optional<Function<Literal, String>> literalFormatter;
    private final Optional<Function<SymbolReference, String>> symbolReferenceFormatter;

    public Formatter(
        Optional<Function<Literal, String>> literalFormatter,
        Optional<Function<SymbolReference, String>> symbolReferenceFormatter) {
      this.literalFormatter = requireNonNull(literalFormatter, "literalFormatter is null");
      this.symbolReferenceFormatter =
          requireNonNull(symbolReferenceFormatter, "symbolReferenceFormatter is null");
    }
  }
}
