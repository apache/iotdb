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

package org.apache.iotdb;

import org.apache.iotdb.db.qp.sql.OldIoTDBSqlParser;
import org.apache.iotdb.db.qp.sql.OldIoTDBSqlParserBaseVisitor;

import org.apache.commons.lang.StringEscapeUtils;

import java.util.List;

public class OldIoTDBSqlVisitor extends OldIoTDBSqlParserBaseVisitor<String[]> {
  @Override
  public String[] visitPrefixPath(OldIoTDBSqlParser.PrefixPathContext ctx) {
    List<OldIoTDBSqlParser.NodeNameContext> nodeNames = ctx.nodeName();
    String[] path = new String[nodeNames.size() + 1];
    path[0] = ctx.ROOT().getText();
    for (int i = 0; i < nodeNames.size(); i++) {
      path[i + 1] = parseNodeName(nodeNames.get(i));
    }
    return path;
  }

  /** function for parsing node name. */
  public String parseNodeName(OldIoTDBSqlParser.NodeNameContext ctx) {
    if (ctx.QUTOED_ID_WITHOUT_DOT() != null) {
      return parseStringWithQuotes(ctx.QUTOED_ID_WITHOUT_DOT().getText());
    } else if (ctx.STRING_LITERAL() != null) {
      return parseStringWithQuotesInNodeName(ctx.STRING_LITERAL().getText());
    } else {
      return ctx.getText();
    }
  }

  private String parseStringWithQuotes(String src) {
    if (2 <= src.length()) {
      if (src.charAt(0) == '\"' && src.charAt(src.length() - 1) == '\"') {
        String unescapeString = StringEscapeUtils.unescapeJava(src.substring(1, src.length() - 1));
        return unescapeString.length() == 0 ? "" : unescapeString.replace("\"\"", "\"");
      }
      if (src.charAt(0) == '`' && src.charAt(src.length() - 1) == '`') {
        String unescapeString = StringEscapeUtils.unescapeJava(src.substring(1, src.length() - 1));
        return unescapeString.length() == 0 ? "" : unescapeString.replace("``", "`");
      }
      if (src.charAt(0) == '\'' && src.charAt(src.length() - 1) == '\'') {
        String unescapeString = StringEscapeUtils.unescapeJava(src.substring(1, src.length() - 1));
        return unescapeString.length() == 0 ? "" : unescapeString.replace("''", "'");
      }
    }
    return src;
  }

  private String parseStringWithQuotesInNodeName(String src) {
    if (2 <= src.length()) {
      if (src.charAt(0) == '\"' && src.charAt(src.length() - 1) == '\"') {
        String unescapeString = StringEscapeUtils.unescapeJava(src.substring(1, src.length() - 1));
        return unescapeString.length() == 0
            ? "\"\""
            : "\"" + unescapeString.replace("\"\"", "\"") + "\"";
      }
      if (src.charAt(0) == '\'' && src.charAt(src.length() - 1) == '\'') {
        String unescapeString = StringEscapeUtils.unescapeJava(src.substring(1, src.length() - 1));
        return unescapeString.length() == 0 ? "''" : "'" + unescapeString.replace("''", "'") + "'";
      }
    }
    return src;
  }
}
