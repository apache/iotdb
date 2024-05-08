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
package org.apache.iotdb.tsfile.common.constant;

import java.util.regex.Pattern;

public class TsFileConstant {

  public static final String TSFILE_SUFFIX = ".tsfile";
  public static final String TSFILE_HOME = "TSFILE_HOME";
  public static final String TSFILE_CONF = "TSFILE_CONF";
  public static final String PATH_ROOT = "root";
  public static final String TMP_SUFFIX = "tmp";
  public static final String PATH_SEPARATOR = ".";
  public static final char PATH_SEPARATOR_CHAR = '.';
  public static final String PATH_SEPARATER_NO_REGEX = "\\.";
  public static final char DOUBLE_QUOTE = '"';
  public static final char BACK_QUOTE = '`';
  public static final String BACK_QUOTE_STRING = "`";
  public static final String DOUBLE_BACK_QUOTE_STRING = "``";

  public static final byte TIME_COLUMN_MASK = (byte) 0x80;
  public static final byte VALUE_COLUMN_MASK = (byte) 0x40;

  // measurementID of aligned time chunk
  public static final String TIME_COLUMN_ID = "";

  private static final String IDENTIFIER_MATCHER = "([a-zA-Z0-9_\\u2E80-\\u9FFF]+)";
  public static final Pattern IDENTIFIER_PATTERN = Pattern.compile(IDENTIFIER_MATCHER);

  private static final String NODE_NAME_MATCHER = "(\\*{0,2}[a-zA-Z0-9_\\u2E80-\\u9FFF]+\\*{0,2})";
  public static final Pattern NODE_NAME_PATTERN = Pattern.compile(NODE_NAME_MATCHER);

  private static final String NODE_NAME_IN_INTO_PATH_MATCHER = "([a-zA-Z0-9_${}\\u2E80-\\u9FFF]+)";
  public static final Pattern NODE_NAME_IN_INTO_PATH_PATTERN =
      Pattern.compile(NODE_NAME_IN_INTO_PATH_MATCHER);

  private TsFileConstant() {}
}
