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

package org.apache.iotdb.commons.queryengine.plan.relational.sql.util;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.parser.CaseInsensitiveStream;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.parser.ParsingException;
import org.apache.iotdb.db.relational.grammar.sql.RelationalSqlLexer;
import org.apache.iotdb.db.relational.grammar.sql.RelationalSqlParser;

import org.antlr.v4.runtime.ANTLRErrorListener;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;

import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.apache.iotdb.db.relational.grammar.sql.RelationalSqlKeywords.sqlKeywords;

public final class ReservedIdentifiers {

  private static final ANTLRErrorListener ERROR_LISTENER =
      new BaseErrorListener() {
        @Override
        public void syntaxError(
            Recognizer<?, ?> recognizer,
            Object offendingSymbol,
            int line,
            int charPositionInLine,
            String message,
            RecognitionException e) {
          throw new ParsingException(message, e, line, charPositionInLine + 1);
        }
      };

  private ReservedIdentifiers() {}

  public static Set<String> reservedIdentifiers() {
    return sqlKeywords().stream()
        .filter(ReservedIdentifiers::reserved)
        .sorted()
        .collect(toImmutableSet());
  }

  public static boolean reserved(final String name) {
    try {
      RelationalSqlLexer lexer =
          new RelationalSqlLexer(new CaseInsensitiveStream(CharStreams.fromString(name)));
      CommonTokenStream tokenStream = new CommonTokenStream(lexer);
      RelationalSqlParser parser = new RelationalSqlParser(tokenStream);

      lexer.removeErrorListeners();
      lexer.addErrorListener(ERROR_LISTENER);

      parser.removeErrorListeners();
      parser.addErrorListener(ERROR_LISTENER);

      parser.identifier();
      return parser.getCurrentToken().getType() != Token.EOF;
    } catch (final ParsingException ignored) {
      return true;
    }
  }
}
