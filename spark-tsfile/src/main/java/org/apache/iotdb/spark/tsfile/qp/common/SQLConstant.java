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
package org.apache.iotdb.spark.tsfile.qp.common;

import java.util.HashMap;
import java.util.Map;

/** this class contains several constants used in SQL. */
public class SQLConstant {

  public static final String DELTA_OBJECT_NAME = "delta_object_name";
  public static final String REGEX_PATH_SEPARATOR = "\\.";
  public static final String PATH_SEPARATOR = ".";
  public static final String RESERVED_TIME = "time";
  public static final String RESERVED_FREQ = "freq";
  public static final String RESERVED_DELTA_OBJECT = "device_name";
  public static final String INT32 = "INT32";
  public static final String INT64 = "INT64";
  public static final String FLOAT = "FLOAT";
  public static final String DOUBLE = "DOUBLE";
  public static final String BYTE_ARRAY = "BYTE_ARRAY";

  public static final int KW_AND = 1;
  public static final int KW_OR = 2;
  public static final int KW_NOT = 3;

  public static final int EQUAL = 11;
  public static final int NOTEQUAL = 12;
  public static final int LESSTHANOREQUALTO = 13;
  public static final int LESSTHAN = 14;
  public static final int GREATERTHANOREQUALTO = 15;
  public static final int GREATERTHAN = 16;
  public static final int EQUAL_NS = 17;

  public static final int TOK_SELECT = 21;
  public static final int TOK_FROM = 22;
  public static final int TOK_WHERE = 23;
  public static final int TOK_INSERT = 24;
  public static final int TOK_DELETE = 25;
  public static final int TOK_UPDATE = 26;
  public static final int TOK_QUERY = 27;

  public static final int TOK_AUTHOR_CREATE = 41;
  public static final int TOK_AUTHOR_DROP = 42;
  public static final int TOK_AUTHOR_GRANT = 43;
  public static final int TOK_AUTHOR_REVOKE = 44;

  public static final int TOK_DATALOAD = 45;

  public static final int TOK_METADATA_CREATE = 51;
  public static final int TOK_METADATA_DELETE = 52;
  public static final int TOK_METADATA_SET_FILE_LEVEL = 53;
  public static final int TOK_PORPERTY_CREATE = 54;
  public static final int TOK_PORPERTY_ADD_LABEL = 55;
  public static final int TOK_PORPERTY_DELETE_LABEL = 56;
  public static final int TOK_PORPERTY_LINK = 57;
  public static final int TOK_PORPERTY_UNLINK = 58;

  public static Map<Integer, String> tokenSymbol = new HashMap<Integer, String>();
  public static Map<Integer, String> tokenNames = new HashMap<Integer, String>();
  public static Map<Integer, Integer> reverseWords = new HashMap<Integer, Integer>();

  static {
    tokenSymbol.put(KW_AND, "&");
    tokenSymbol.put(KW_OR, "|");
    tokenSymbol.put(KW_NOT, "!");
    tokenSymbol.put(EQUAL, "=");
    tokenSymbol.put(NOTEQUAL, "<>");
    tokenSymbol.put(EQUAL_NS, "<=>");
    tokenSymbol.put(LESSTHANOREQUALTO, "<=");
    tokenSymbol.put(LESSTHAN, "<");
    tokenSymbol.put(GREATERTHANOREQUALTO, ">=");
    tokenSymbol.put(GREATERTHAN, ">");
  }

  static {
    tokenNames.put(KW_AND, "and");
    tokenNames.put(KW_OR, "or");
    tokenNames.put(KW_NOT, "not");
    tokenNames.put(EQUAL, "equal");
    tokenNames.put(NOTEQUAL, "not_equal");
    tokenNames.put(EQUAL_NS, "equal_ns");
    tokenNames.put(LESSTHANOREQUALTO, "lessthan_or_equalto");
    tokenNames.put(LESSTHAN, "lessthan");
    tokenNames.put(GREATERTHANOREQUALTO, "greaterthan_or_equalto");
    tokenNames.put(GREATERTHAN, "greaterthan");

    tokenNames.put(TOK_SELECT, "TOK_SELECT");
    tokenNames.put(TOK_FROM, "TOK_FROM");
    tokenNames.put(TOK_WHERE, "TOK_WHERE");
    tokenNames.put(TOK_INSERT, "TOK_INSERT");
    tokenNames.put(TOK_DELETE, "TOK_DELETE");
    tokenNames.put(TOK_UPDATE, "TOK_UPDATE");
    tokenNames.put(TOK_QUERY, "TOK_QUERY");

    tokenNames.put(TOK_AUTHOR_CREATE, "TOK_AUTHOR_CREATE");
    tokenNames.put(TOK_AUTHOR_DROP, "TOK_AUTHOR_DROP");
    tokenNames.put(TOK_AUTHOR_GRANT, "TOK_AUTHOR_GRANT");
    tokenNames.put(TOK_AUTHOR_REVOKE, "TOK_AUTHOR_REVOKE");
    tokenNames.put(TOK_DATALOAD, "TOK_DATALOAD");

    tokenNames.put(TOK_METADATA_CREATE, "TOK_METADATA_CREATE");
    tokenNames.put(TOK_METADATA_DELETE, "TOK_METADATA_DELETE");
    tokenNames.put(TOK_METADATA_SET_FILE_LEVEL, "TOK_METADATA_SET_FILE_LEVEL");
    tokenNames.put(TOK_PORPERTY_CREATE, "TOK_PORPERTY_CREATE");
    tokenNames.put(TOK_PORPERTY_ADD_LABEL, "TOK_PORPERTY_ADD_LABEL");
    tokenNames.put(TOK_PORPERTY_DELETE_LABEL, "TOK_PORPERTY_DELETE_LABEL");
    tokenNames.put(TOK_PORPERTY_LINK, "TOK_PORPERTY_LINK");
    tokenNames.put(TOK_PORPERTY_UNLINK, "TOK_PORPERTY_UNLINK");
  }

  static {
    reverseWords.put(KW_AND, KW_OR);
    reverseWords.put(KW_OR, KW_AND);
    reverseWords.put(EQUAL, NOTEQUAL);
    reverseWords.put(NOTEQUAL, EQUAL);
    reverseWords.put(LESSTHAN, GREATERTHANOREQUALTO);
    reverseWords.put(GREATERTHANOREQUALTO, LESSTHAN);
    reverseWords.put(LESSTHANOREQUALTO, GREATERTHAN);
    reverseWords.put(GREATERTHAN, LESSTHANOREQUALTO);
  }

  public static boolean isReservedPath(String pathStr) {
    return pathStr.equals(SQLConstant.RESERVED_TIME)
        || pathStr.equals(SQLConstant.RESERVED_FREQ)
        || pathStr.equals(SQLConstant.RESERVED_DELTA_OBJECT);
  }
}
