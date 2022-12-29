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

package org.apache.iotdb.db.mpp.plan.parser;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.misc.IntervalSet;
import org.antlr.v4.runtime.misc.ParseCancellationException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class SqlParseError extends BaseErrorListener {

  public static final SqlParseError INSTANCE = new SqlParseError();

  @Override
  public void syntaxError(
      Recognizer<?, ?> recognizer,
      Object offendingSymbol,
      int line,
      int charPositionInLine,
      String msg,
      RecognitionException e) {
    // make msg clearer
    if (recognizer instanceof Parser) {
      IntervalSet expectedTokens = ((Parser) recognizer).getExpectedTokens();
      String expectedTokensString = expectedTokens.toString(recognizer.getVocabulary());
      String trimmed = expectedTokensString.replace(" ", "");
      Set<String> expectedTokenNames =
          new HashSet<>(Arrays.asList(trimmed.substring(1, trimmed.length() - 1).split(",")));

      if (expectedTokenNames.contains("ID") && expectedTokenNames.contains("QUOTED_ID")) {
        // node name
        if (expectedTokenNames.contains("*") && expectedTokenNames.contains("**")) {
          msg = msg.replace(expectedTokensString, "{ID, QUOTED_ID, *, **}");
        } else {
          msg = msg.replace(expectedTokensString, "{ID, QUOTED_ID}");
        }
      }
    }

    throw new ParseCancellationException("line " + line + ":" + charPositionInLine + " " + msg);
  }
}
