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
package org.apache.iotdb.influxdb.qp.strategy;

import org.apache.iotdb.influxdb.qp.logical.Operator;
import org.apache.iotdb.influxdb.qp.sql.InfluxDBLexer;
import org.apache.iotdb.influxdb.qp.sql.InfluxDBParser;
import org.apache.iotdb.influxdb.qp.sql.InfluxDBSqlVisitor;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;

/** LogicalGenerator. */
public class LogicalGenerator {
  public static Operator generate(String sql) throws ParseCancellationException {
    InfluxDBSqlVisitor influxDBSqlVisitor = new InfluxDBSqlVisitor();
    CharStream charStream1 = CharStreams.fromString(sql);
    InfluxDBLexer lexer1 = new InfluxDBLexer(charStream1);
    lexer1.removeErrorListeners();
    lexer1.addErrorListener(SQLParseError.INSTANCE);
    CommonTokenStream tokens1 = new CommonTokenStream(lexer1);
    InfluxDBParser parser1 = new InfluxDBParser(tokens1);
    parser1.getInterpreter().setPredictionMode(PredictionMode.SLL);
    parser1.removeErrorListeners();
    parser1.addErrorListener(SQLParseError.INSTANCE);
    ParseTree tree;
    try {
      tree = parser1.singleStatement(); // STAGE 1
    } catch (Exception ex) {
      CharStream charStream2 = CharStreams.fromString(sql);
      InfluxDBLexer lexer2 = new InfluxDBLexer(charStream2);
      lexer2.removeErrorListeners();
      lexer2.addErrorListener(SQLParseError.INSTANCE);
      CommonTokenStream tokens2 = new CommonTokenStream(lexer2);
      InfluxDBParser parser2 = new InfluxDBParser(tokens2);
      parser2.getInterpreter().setPredictionMode(PredictionMode.LL);
      parser2.removeErrorListeners();
      parser2.addErrorListener(SQLParseError.INSTANCE);
      tree = parser2.singleStatement(); // STAGE 2
      // if we parse ok, it's LL not SLL
    }
    return influxDBSqlVisitor.visit(tree);
  }

  private LogicalGenerator() {}
}
