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
package org.apache.iotdb.db.qp.constant;

import java.util.HashMap;
import java.util.Map;

import org.apache.iotdb.db.sql.parse.TqlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TqlParserConstant {

  private static final Logger logger = LoggerFactory.getLogger(TqlParserConstant.class);

  private TqlParserConstant() {
    // forbidding instantiation
  }

  private static Map<Integer, Integer> antlrQpMap = new HashMap<>();

  // used to get operator type when construct operator from AST Tree
  static {
    antlrQpMap.put(TqlParser.OPERATOR_AND, SQLConstant.KW_AND);
    antlrQpMap.put(TqlParser.OPERATOR_OR, SQLConstant.KW_OR);
    antlrQpMap.put(TqlParser.OPERATOR_NOT, SQLConstant.KW_NOT);

    antlrQpMap.put(TqlParser.OPERATOR_EQ, SQLConstant.EQUAL);
    antlrQpMap.put(TqlParser.OPERATOR_NEQ, SQLConstant.NOTEQUAL);
    antlrQpMap.put(TqlParser.OPERATOR_LTE, SQLConstant.LESSTHANOREQUALTO);
    antlrQpMap.put(TqlParser.OPERATOR_LT, SQLConstant.LESSTHAN);
    antlrQpMap.put(TqlParser.OPERATOR_GTE, SQLConstant.GREATERTHANOREQUALTO);
    antlrQpMap.put(TqlParser.OPERATOR_GT, SQLConstant.GREATERTHAN);
//    antlrQpMap.put(TqlParser.EQUAL_NS, SQLConstant.EQUAL_NS);

    antlrQpMap.put(TqlParser.TOK_SELECT, SQLConstant.TOK_SELECT);
    antlrQpMap.put(TqlParser.TOK_FROM, SQLConstant.TOK_FROM);
    antlrQpMap.put(TqlParser.TOK_WHERE, SQLConstant.TOK_WHERE);
    antlrQpMap.put(TqlParser.TOK_QUERY, SQLConstant.TOK_QUERY);
  }

  /**
   * return map value corresponding to key,when not contain the param,print it.
   *
   * @param antlrIntType -param to judge whether antlrQpMap has key
   * @return -map value corresponding to the param
   */
  public static int getTSTokenIntType(int antlrIntType) {
    if (!antlrQpMap.containsKey(antlrIntType)) {
      logger.error("No such TSToken: {}", antlrIntType);
    }
    return antlrQpMap.get(antlrIntType);
  }

}
