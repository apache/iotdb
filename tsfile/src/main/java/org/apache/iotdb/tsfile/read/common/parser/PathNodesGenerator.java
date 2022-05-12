/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.tsfile.read.common.parser;

import org.apache.iotdb.db.qp.sql.PathParser;
import org.apache.iotdb.db.qp.sql.SqlLexer;
import org.apache.iotdb.tsfile.exception.PathParseException;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;

/** convert String path to String[] nodes * */
public class PathNodesGenerator {
  private static PathVisitor pathVisitor = new PathVisitor();

  public static String[] splitPathToNodes(String path) throws PathParseException {
    try {
      return invokeParser(path);
    } catch (ParseCancellationException e) {
      throw new PathParseException(path);
    }
  }

  /** throw exception if path is illegal. */
  public static void checkPath(String path) throws PathParseException {
    try {
      invokeParser(path);
    } catch (ParseCancellationException e) {
      throw new PathParseException(path);
    }
  }

  private static String[] invokeParser(String path) {

    CharStream charStream1 = CharStreams.fromString(path);

    SqlLexer lexer1 = new SqlLexer(charStream1);
    lexer1.removeErrorListeners();
    lexer1.addErrorListener(PathParseError.INSTANCE);

    CommonTokenStream tokens1 = new CommonTokenStream(lexer1);

    PathParser pathParser1 = new PathParser(tokens1);
    pathParser1.getInterpreter().setPredictionMode(PredictionMode.SLL);
    pathParser1.removeErrorListeners();
    pathParser1.addErrorListener(PathParseError.INSTANCE);

    ParseTree tree;
    try {
      // STAGE 1: try with simpler/faster SLL(*)
      tree = pathParser1.path();
      // if we get here, there was no syntax error and SLL(*) was enough;
      // there is no need to try full LL(*)
    } catch (Exception ex) {
      CharStream charStream2 = CharStreams.fromString(path);

      SqlLexer lexer2 = new SqlLexer(charStream2);
      lexer2.removeErrorListeners();
      lexer2.addErrorListener(PathParseError.INSTANCE);

      CommonTokenStream tokens2 = new CommonTokenStream(lexer2);

      PathParser pathParser2 = new PathParser(tokens2);
      pathParser2.getInterpreter().setPredictionMode(PredictionMode.LL);
      pathParser2.removeErrorListeners();
      pathParser2.addErrorListener(PathParseError.INSTANCE);

      // STAGE 2: parser with full LL(*)
      tree = pathParser2.path();
      // if we get here, it's LL not SLL
    }
    return pathVisitor.visit(tree);
  }
}
