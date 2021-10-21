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

package org.apache.iotdb.db.qp.constant;

import org.apache.iotdb.db.qp.sql.IoTDBSqlLexer;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

public class FilterConstant {

  public static final Map<Integer, FilterType> lexerToFilterType = new HashMap<>();
  public static final Map<FilterType, String> filterSymbol = new EnumMap<>(FilterType.class);
  public static final Map<FilterType, String> filterNames = new EnumMap<>(FilterType.class);
  public static final Map<FilterType, FilterType> filterReverseWords =
      new EnumMap<>(FilterType.class);

  public enum FilterType {
    KW_AND,
    KW_OR,
    KW_NOT,

    EQUAL,
    NOTEQUAL,
    LESSTHANOREQUALTO,
    LESSTHAN,
    GREATERTHANOREQUALTO,
    GREATERTHAN,
    IN,
    REGEXP,
    LIKE
  }

  static {
    lexerToFilterType.put(IoTDBSqlLexer.OPERATOR_EQ, FilterType.EQUAL);
    lexerToFilterType.put(IoTDBSqlLexer.OPERATOR_NEQ, FilterType.NOTEQUAL);
    lexerToFilterType.put(IoTDBSqlLexer.OPERATOR_LTE, FilterType.LESSTHANOREQUALTO);
    lexerToFilterType.put(IoTDBSqlLexer.OPERATOR_LT, FilterType.LESSTHAN);
    lexerToFilterType.put(IoTDBSqlLexer.OPERATOR_GTE, FilterType.GREATERTHANOREQUALTO);
    lexerToFilterType.put(IoTDBSqlLexer.OPERATOR_GT, FilterType.GREATERTHAN);
    lexerToFilterType.put(IoTDBSqlLexer.OPERATOR_IN, FilterType.IN);
    lexerToFilterType.put(IoTDBSqlLexer.REGEXP, FilterType.REGEXP);
    lexerToFilterType.put(IoTDBSqlLexer.LIKE, FilterType.LIKE);
  }

  static {
    filterSymbol.put(FilterType.KW_AND, "&");
    filterSymbol.put(FilterType.KW_OR, "|");
    filterSymbol.put(FilterType.KW_NOT, "!");
    filterSymbol.put(FilterType.EQUAL, "=");
    filterSymbol.put(FilterType.NOTEQUAL, "<>");
    filterSymbol.put(FilterType.LESSTHANOREQUALTO, "<=");
    filterSymbol.put(FilterType.LESSTHAN, "<");
    filterSymbol.put(FilterType.GREATERTHANOREQUALTO, ">=");
    filterSymbol.put(FilterType.GREATERTHAN, ">");
    filterSymbol.put(FilterType.LIKE, "%");
  }

  static {
    filterNames.put(FilterType.KW_AND, "and");
    filterNames.put(FilterType.KW_OR, "or");
    filterNames.put(FilterType.KW_NOT, "not");
    filterNames.put(FilterType.EQUAL, "equal");
    filterNames.put(FilterType.NOTEQUAL, "not_equal");
    filterNames.put(FilterType.LESSTHANOREQUALTO, "lessthan_or_equalto");
    filterNames.put(FilterType.LESSTHAN, "lessthan");
    filterNames.put(FilterType.GREATERTHANOREQUALTO, "greaterthan_or_equalto");
    filterNames.put(FilterType.GREATERTHAN, "greaterthan");
    filterNames.put(FilterType.IN, "in");
    filterNames.put(FilterType.REGEXP, "regexp");
    filterNames.put(FilterType.LIKE, "like");
  }

  static {
    filterReverseWords.put(FilterType.KW_AND, FilterType.KW_OR);
    filterReverseWords.put(FilterType.KW_OR, FilterType.KW_AND);
    filterReverseWords.put(FilterType.EQUAL, FilterType.NOTEQUAL);
    filterReverseWords.put(FilterType.NOTEQUAL, FilterType.EQUAL);
    filterReverseWords.put(FilterType.LESSTHAN, FilterType.GREATERTHANOREQUALTO);
    filterReverseWords.put(FilterType.GREATERTHANOREQUALTO, FilterType.LESSTHAN);
    filterReverseWords.put(FilterType.LESSTHANOREQUALTO, FilterType.GREATERTHAN);
    filterReverseWords.put(FilterType.GREATERTHAN, FilterType.LESSTHANOREQUALTO);
  }
}
