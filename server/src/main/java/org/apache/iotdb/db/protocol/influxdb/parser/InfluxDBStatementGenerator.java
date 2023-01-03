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

package org.apache.iotdb.db.protocol.influxdb.parser;

import org.apache.iotdb.db.mpp.plan.parser.SqlParseError;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.qp.sql.InfluxDBSqlParser;
import org.apache.iotdb.db.qp.sql.SqlLexer;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;

public class InfluxDBStatementGenerator {

  private InfluxDBStatementGenerator() {}

  public static Statement generate(String sql) throws ParseCancellationException {
    InfluxDBAstVisitor influxDBAstVisitor = new InfluxDBAstVisitor();
    CharStream charStream1 = CharStreams.fromString(sql);
    SqlLexer lexer1 = new SqlLexer(charStream1);
    lexer1.removeErrorListeners();
    lexer1.addErrorListener(SqlParseError.INSTANCE);
    CommonTokenStream tokens1 = new CommonTokenStream(lexer1);
    InfluxDBSqlParser parser1 = new InfluxDBSqlParser(tokens1);
    parser1.getInterpreter().setPredictionMode(PredictionMode.SLL);
    parser1.removeErrorListeners();
    parser1.addErrorListener(SqlParseError.INSTANCE);
    ParseTree tree;
    try {
      tree = parser1.singleStatement(); // STAGE 1
    } catch (Exception ex) {
      CharStream charStream2 = CharStreams.fromString(sql);
      SqlLexer lexer2 = new SqlLexer(charStream2);
      lexer2.removeErrorListeners();
      lexer2.addErrorListener(SqlParseError.INSTANCE);
      CommonTokenStream tokens2 = new CommonTokenStream(lexer2);
      InfluxDBSqlParser parser2 = new InfluxDBSqlParser(tokens2);
      parser2.getInterpreter().setPredictionMode(PredictionMode.LL);
      parser2.removeErrorListeners();
      parser2.addErrorListener(SqlParseError.INSTANCE);
      tree = parser2.singleStatement(); // STAGE 2
      // if we parse ok, it's LL not SLL
    }
    return influxDBAstVisitor.visit(tree);
  }
}
