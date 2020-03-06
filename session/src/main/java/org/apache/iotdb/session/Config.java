/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.session;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_ROOT;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

import java.util.regex.Pattern;

public class Config {

  public static final String DEFAULT_USER = "user";
  public static final String DEFAULT_PASSWORD = "password";
  public static final int DEFAULT_FETCH_SIZE = 10000;
  public static final int DEFAULT_TIMEOUT_MS = 0;

  public static final String NODE_MATCHER = "[" + PATH_SEPARATOR + "]" + "([a-zA-Z0-9_]+)";

  // for path like: root.sg1.d1."1.2.3" or root.sg1.d1.'1.2.3', only occurs in the end of the path and only occurs once
  public static final String NODE_WITH_QUOTATION_MARK_MATCHER =
      "[" + PATH_SEPARATOR + "][\"|\']([a-zA-Z0-9_]+)(" + NODE_MATCHER + ")+[\"|\']";
  public static final Pattern PATH_PATTERN = Pattern
      .compile(PATH_ROOT + "(" + NODE_MATCHER + ")+(" + NODE_WITH_QUOTATION_MARK_MATCHER + ")?");

}
