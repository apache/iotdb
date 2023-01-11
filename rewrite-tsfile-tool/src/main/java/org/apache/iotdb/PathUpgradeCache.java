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

import org.apache.iotdb.db.qp.sql.OldIoTDBSqlLexer;
import org.apache.iotdb.db.qp.sql.OldIoTDBSqlParser;
import org.apache.iotdb.db.qp.strategy.SQLParseError;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.HashMap;
import java.util.Map;

public class PathUpgradeCache {
  private final boolean needUpgrade;
  private final Map<String, String[]> paths;

  public PathUpgradeCache(boolean needUpgrade) {
    this.needUpgrade = needUpgrade;
    this.paths = new HashMap<>();
  }

  public String getPath(String path) {
    if (!needUpgrade) {
      return path;
    }
    if (paths.containsKey(path)) {
      return String.join(".", paths.get(path));
    }
    return String.join(".", parsePath(path));
  }

  public String[] getNodes(String path) {
    if (!needUpgrade) {
      return path.split("\\.");
    }
    if (paths.containsKey(path)) {
      return paths.get(path);
    }
    return parsePath(path);
  }

  private String[] parsePath(String path) {

    OldIoTDBSqlVisitor ioTDBSqlVisitor = new OldIoTDBSqlVisitor();

    CharStream charStream1 = CharStreams.fromString(path);

    OldIoTDBSqlLexer lexer1 = new OldIoTDBSqlLexer(charStream1);
    lexer1.removeErrorListeners();
    lexer1.addErrorListener(SQLParseError.INSTANCE);

    CommonTokenStream tokens1 = new CommonTokenStream(lexer1);

    OldIoTDBSqlParser parser1 = new OldIoTDBSqlParser(tokens1);
    parser1.getInterpreter().setPredictionMode(PredictionMode.SLL);
    parser1.removeErrorListeners();
    parser1.addErrorListener(SQLParseError.INSTANCE);

    ParseTree tree;
    try {
      tree = parser1.prefixPath();
    } catch (Exception ex) {
      CharStream charStream2 = CharStreams.fromString(path);

      OldIoTDBSqlLexer lexer2 = new OldIoTDBSqlLexer(charStream2);
      lexer2.removeErrorListeners();
      lexer2.addErrorListener(SQLParseError.INSTANCE);

      CommonTokenStream tokens2 = new CommonTokenStream(lexer2);

      OldIoTDBSqlParser parser2 = new OldIoTDBSqlParser(tokens2);
      parser2.getInterpreter().setPredictionMode(PredictionMode.LL);
      parser2.removeErrorListeners();
      parser2.addErrorListener(SQLParseError.INSTANCE);

      // STAGE 2: parser with full LL(*)
      tree = parser2.prefixPath();
      // if we get here, it's LL not SLL
    }
    String[] nodes = ioTDBSqlVisitor.visit(tree);
    for (int i = 0; i < nodes.length; i++) {
      String replacedNodeName = nodes[i].replace("`", "``");
      nodes[i] = containIllegalChar(nodes[i]) ? "`" + replacedNodeName + "`" : replacedNodeName;
    }
    return paths.putIfAbsent(path, nodes);
  }

  private boolean containIllegalChar(String nodeName) {
    boolean nonDigital = false;
    for (int i = 0; i < nodeName.length(); i++) {
      char c = nodeName.charAt(i);
      if (!Character.isLetterOrDigit(c) && (c < '\u2E80' || c > '\u9FFF')) {
        return true;
      }
      if (!Character.isDigit(c)) {
        nonDigital = true;
      }
    }
    return !nonDigital;
  }
}
