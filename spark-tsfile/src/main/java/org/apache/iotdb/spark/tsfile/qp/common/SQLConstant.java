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

import org.spark_project.guava.collect.ImmutableMap;

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
  public static final int TOK_PROPERTY_CREATE = 54;
  public static final int TOK_PROPERTY_ADD_LABEL = 55;
  public static final int TOK_PROPERTY_DELETE_LABEL = 56;
  public static final int TOK_PROPERTY_LINK = 57;
  public static final int TOK_PROPERTY_UNLINK = 58;

  public static final Map<Integer, String> tokenSymbol =
      new ImmutableMap.Builder<Integer, String>()
          .put(KW_AND, "&")
          .put(KW_OR, "|")
          .put(KW_NOT, "!")
          .put(EQUAL, "=")
          .put(NOTEQUAL, "<>")
          .put(EQUAL_NS, "<=>")
          .put(LESSTHANOREQUALTO, "<=")
          .put(LESSTHAN, "<")
          .put(GREATERTHANOREQUALTO, ">=")
          .put(GREATERTHAN, ">")
          .build();
  public static final Map<Integer, String> tokenNames =
      new ImmutableMap.Builder<Integer, String>()
          .put(KW_AND, "and")
          .put(KW_OR, "or")
          .put(KW_NOT, "not")
          .put(EQUAL, "equal")
          .put(NOTEQUAL, "not_equal")
          .put(EQUAL_NS, "equal_ns")
          .put(LESSTHANOREQUALTO, "lessthan_or_equalto")
          .put(LESSTHAN, "lessthan")
          .put(GREATERTHANOREQUALTO, "greaterthan_or_equalto")
          .put(GREATERTHAN, "greaterthan")
          .put(TOK_SELECT, "TOK_SELECT")
          .put(TOK_FROM, "TOK_FROM")
          .put(TOK_WHERE, "TOK_WHERE")
          .put(TOK_INSERT, "TOK_INSERT")
          .put(TOK_DELETE, "TOK_DELETE")
          .put(TOK_UPDATE, "TOK_UPDATE")
          .put(TOK_QUERY, "TOK_QUERY")
          .put(TOK_AUTHOR_CREATE, "TOK_AUTHOR_CREATE")
          .put(TOK_AUTHOR_DROP, "TOK_AUTHOR_DROP")
          .put(TOK_AUTHOR_GRANT, "TOK_AUTHOR_GRANT")
          .put(TOK_AUTHOR_REVOKE, "TOK_AUTHOR_REVOKE")
          .put(TOK_DATALOAD, "TOK_DATALOAD")
          .put(TOK_METADATA_CREATE, "TOK_METADATA_CREATE")
          .put(TOK_METADATA_DELETE, "TOK_METADATA_DELETE")
          .put(TOK_METADATA_SET_FILE_LEVEL, "TOK_METADATA_SET_FILE_LEVEL")
          .put(TOK_PROPERTY_CREATE, "TOK_PROPERTY_CREATE")
          .put(TOK_PROPERTY_ADD_LABEL, "TOK_PROPERTY_ADD_LABEL")
          .put(TOK_PROPERTY_DELETE_LABEL, "TOK_PROPERTY_DELETE_LABEL")
          .put(TOK_PROPERTY_LINK, "TOK_PROPERTY_LINK")
          .put(TOK_PROPERTY_UNLINK, "TOK_PROPERTY_UNLINK")
          .build();
  public static final Map<Integer, Integer> reverseWords =
      new ImmutableMap.Builder<Integer, Integer>()
          .put(KW_AND, KW_OR)
          .put(KW_OR, KW_AND)
          .put(EQUAL, NOTEQUAL)
          .put(NOTEQUAL, EQUAL)
          .put(LESSTHAN, GREATERTHANOREQUALTO)
          .put(GREATERTHANOREQUALTO, LESSTHAN)
          .put(LESSTHANOREQUALTO, GREATERTHAN)
          .put(GREATERTHAN, LESSTHANOREQUALTO)
          .build();

  public static boolean isReservedPath(String pathStr) {
    return pathStr.equals(SQLConstant.RESERVED_TIME)
        || pathStr.equals(SQLConstant.RESERVED_FREQ)
        || pathStr.equals(SQLConstant.RESERVED_DELTA_OBJECT);
  }
}
