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

package org.apache.iotdb.db.queryengine.plan.relational.sql.parser;

import org.apache.iotdb.db.i18n.DataNodeQueryMessages;

import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.atn.ATN;
import org.antlr.v4.runtime.atn.LexerATNSimulator;
import org.antlr.v4.runtime.atn.ParserATNSimulator;
import org.antlr.v4.runtime.atn.PredictionContextCache;
import org.antlr.v4.runtime.dfa.DFA;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class AntlrATNCacheFields {
  private final ATN atn;
  private final PredictionContextCache predictionContextCache;
  private final DFA[] decisionToDFA;

  public AntlrATNCacheFields(ATN atn) {
    this.atn = requireNonNull(atn, DataNodeQueryMessages.EXCEPTION_ATN_IS_NULL_48BE0D3E);
    this.predictionContextCache = new PredictionContextCache();
    this.decisionToDFA = createDecisionToDFA(atn);
  }

  @SuppressWarnings("ObjectEquality")
  public void configureLexer(Lexer lexer) {
    requireNonNull(lexer, DataNodeQueryMessages.EXCEPTION_LEXER_IS_NULL_88834E18);
    // Intentional identity equals comparison
    checkArgument(
        atn == lexer.getATN(),
        DataNodeQueryMessages
            .EXCEPTION_LEXER_ATN_MISMATCH_COLON_EXPECTED_ARG_COMMA_FOUND_ARG_8ED22CF1,
        atn,
        lexer.getATN());
    lexer.setInterpreter(new LexerATNSimulator(lexer, atn, decisionToDFA, predictionContextCache));
  }

  @SuppressWarnings("ObjectEquality")
  public void configureParser(Parser parser) {
    requireNonNull(parser, DataNodeQueryMessages.EXCEPTION_PARSER_IS_NULL_AE8E5D6F);
    // Intentional identity equals comparison
    checkArgument(
        atn == parser.getATN(),
        DataNodeQueryMessages
            .EXCEPTION_PARSER_ATN_MISMATCH_COLON_EXPECTED_ARG_COMMA_FOUND_ARG_FF75D61B,
        atn,
        parser.getATN());
    parser.setInterpreter(
        new ParserATNSimulator(parser, atn, decisionToDFA, predictionContextCache));
  }

  private static DFA[] createDecisionToDFA(ATN atn) {
    DFA[] decisionToDFA = new DFA[atn.getNumberOfDecisions()];
    for (int i = 0; i < decisionToDFA.length; i++) {
      decisionToDFA[i] = new DFA(atn.getDecisionState(i), i);
    }
    return decisionToDFA;
  }
}
