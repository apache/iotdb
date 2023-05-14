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

package org.apache.iotdb.cli;

import org.apache.iotdb.cli.utils.JlineUtils;
import org.apache.iotdb.db.qp.sql.IoTDBSqlLexer;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.Token;
import org.jline.reader.Highlighter;
import org.jline.reader.LineReader;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;

import java.util.regex.Pattern;

import static org.jline.utils.AttributedStyle.DEFAULT;
import static org.jline.utils.AttributedStyle.GREEN;

public class IoTDBSyntaxHighlighter implements Highlighter {

  private static final AttributedStyle KEYWORD_STYLE = DEFAULT.foreground(GREEN);

  @Override
  public AttributedString highlight(LineReader reader, String buffer) {
    CharStream stream = CharStreams.fromString(buffer);
    IoTDBSqlLexer tokenSource = new IoTDBSqlLexer(stream);
    tokenSource.removeErrorListeners();
    AttributedStringBuilder builder = new AttributedStringBuilder();
    while (true) {
      Token token = tokenSource.nextToken();
      int type = token.getType();
      if (type == Token.EOF) {
        break;
      }
      String text = token.getText();

      if (isKeyword(text)) {
        builder.styled(KEYWORD_STYLE, text);
      } else {
        builder.append(text);
      }
    }

    return builder.toAttributedString();
  }

  @Override
  public void setErrorPattern(Pattern errorPattern) {}

  @Override
  public void setErrorIndex(int errorIndex) {}

  private boolean isKeyword(String token) {
    return JlineUtils.SQL_KEYWORDS.contains(token.toUpperCase());
  }
}
