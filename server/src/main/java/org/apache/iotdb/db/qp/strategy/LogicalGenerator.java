/**
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

import main.antlr4.org.apache.iotdb.db.sql.parse.TSLexer;
import main.antlr4.org.apache.iotdb.db.sql.parse.TSParser;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.iotdb.db.qp.logical.RootOperator;

import java.time.ZoneId;

/**
 * This class receives a query String and transform it to an operator which is a logical plan.
 */
public class LogicalGenerator {

  private static final String ERR_INCORRECT_AUTHOR_COMMAND = "illegal ast tree in grant author "
      + "command, please check you SQL statement";

  private RootOperator initializedOperator = null;
  private ZoneId zoneId;

  public LogicalGenerator(ZoneId zoneId) {
    this.zoneId = zoneId;
  }

  public RootOperator getLogicalPlan(String command){
    RootOperator r = null;
    CharStream input = CharStreams.fromString(command);
    TSLexer lexer = new TSLexer(input);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    TSParser parser = new TSParser(tokens);
    parser.removeErrorListeners();
    parser.addErrorListener(new ExecuteSqlErrorListener());
    ParseTree tree = parser.statement();
    ExecuteSqlVisitor visitor = new ExecuteSqlVisitor(zoneId);
    r = (RootOperator) visitor.visit(tree);
    return r;
  }
}
