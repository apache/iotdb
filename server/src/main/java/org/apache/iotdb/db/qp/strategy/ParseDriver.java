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
package org.apache.iotdb.db.qp.strategy;

import java.time.ZoneId;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.iotdb.db.qp.logical.Operator;

/**
 * ParseDriver.
 *
 */
public class ParseDriver {
  private ParseTreeWalker walker;

  public ParseDriver() {
    walker = new ParseTreeWalker();
  }

  public Operator parse(String sql, ZoneId zoneId) throws ParseCancellationException {
    LogicalGenerator logicalGenerator = new LogicalGenerator(zoneId);
    CharStream charStream1 = CharStreams.fromString(sql);
    SqlBaseLexer lexer1 = new SqlBaseLexer(charStream1);
    CommonTokenStream tokens1 = new CommonTokenStream(lexer1);
    SqlBaseParser parser1 = new SqlBaseParser(tokens1);
    parser1.getInterpreter().setPredictionMode(PredictionMode.SLL);
    parser1.removeErrorListeners();
    parser1.addErrorListener(LogicalGeneratorError.INSTANCE);
    ParseTree tree;
    try {
      tree = parser1.singleStatement();  // STAGE 1
    }
    catch (Exception ex) {
      CharStream charStream2 = CharStreams.fromString(sql);
      SqlBaseLexer lexer2 = new SqlBaseLexer(charStream2);
      CommonTokenStream tokens2 = new CommonTokenStream(lexer2);
      SqlBaseParser parser2 = new SqlBaseParser(tokens2);
      parser2.getInterpreter().setPredictionMode(PredictionMode.LL);
      parser2.removeErrorListeners();
      parser2.addErrorListener(LogicalGeneratorError.INSTANCE);
      tree = parser2.singleStatement();  // STAGE 2
      // if we parse ok, it's LL not SLL
    }
    walker.walk(logicalGenerator, tree);
    return logicalGenerator.getLogicalPlan();
  }
}
