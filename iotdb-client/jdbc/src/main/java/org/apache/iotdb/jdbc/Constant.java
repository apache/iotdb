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
package org.apache.iotdb.jdbc;

public class Constant {

  private Constant() {}

  public static final String GLOBAL_DB_NAME = "IoTDB";

  static final String METHOD_NOT_SUPPORTED = "Method not supported";
  static final String PARAMETER_NOT_NULL = "The parameter cannot be null";
  static final String PARAMETER_SUPPORTED =
      "Parameter only supports BOOLEAN,INT32,INT64,FLOAT,DOUBLE,TEXT data type";

  public static final String STATISTICS_PATHNUM = "* Num of series paths: %d";
  public static final String STATISTICS_SEQFILENUM = "* Num of sequence files read: %d";
  public static final String STATISTICS_UNSEQFILENUM = "* Num of unsequence files read: %d";
  public static final String STATISTICS_SEQCHUNKINFO =
      "* Num of sequence chunks: %d, avg points: %.1f";
  public static final String STATISTICS_UNSEQCHUNKINFO =
      "* Num of unsequence chunks: %d, avg points: %.1f";
  public static final String STATISTICS_PAGEINFO =
      "* Num of Pages: %d, overlapped pages: %d (%.1f%%)";
  public static final String STATISTICS_RESULT_LINES = "* Lines of result: %d";
  public static final String STATISTICS_PRC_INFO = "* Num of RPC: %d, avg cost: %d ms";

  // version number
  public enum Version {
    V_0_12,
    V_0_13,
    V_1_0
  }
}
