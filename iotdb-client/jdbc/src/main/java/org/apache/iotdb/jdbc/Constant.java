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

import org.apache.iotdb.jdbc.i18n.JdbcMessages;

public class Constant {

  private Constant() {}

  public static final String GLOBAL_DB_NAME = "IoTDB";
  public static final String TABLE_DIALECT = "table";
  public static final String TREE_DIALECT = "tree";

  public static final String METHOD_NOT_SUPPORTED = JdbcMessages.METHOD_NOT_SUPPORTED;
  static final String PARAMETER_NOT_NULL =
      JdbcMessages.EXCEPTION_THE_PARAMETER_CANNOT_BE_NULL_6CD5B7DE;
  static final String PARAMETER_SUPPORTED =
      JdbcMessages
          .EXCEPTION_PARAMETER_ONLY_SUPPORTS_BOOLEAN_INT32_INT64_FLOAT_DOUBLE_TEXT_DATA_TYPE_58CD5656;

  public static final String STATISTICS_PATHNUM =
      JdbcMessages.EXCEPTION_NUM_OF_SERIES_PATHS_ARG_5AAACC65;
  public static final String STATISTICS_SEQFILENUM =
      JdbcMessages.EXCEPTION_NUM_OF_SEQUENCE_FILES_READ_ARG_002C1609;
  public static final String STATISTICS_UNSEQFILENUM =
      JdbcMessages.EXCEPTION_NUM_OF_UNSEQUENCE_FILES_READ_ARG_686A2478;
  public static final String STATISTICS_SEQCHUNKINFO =
      JdbcMessages.EXCEPTION_NUM_OF_SEQUENCE_CHUNKS_ARG_AVG_POINTS_ARG_B119F7E6;
  public static final String STATISTICS_UNSEQCHUNKINFO =
      JdbcMessages.EXCEPTION_NUM_OF_UNSEQUENCE_CHUNKS_ARG_AVG_POINTS_ARG_723DCD8F;
  public static final String STATISTICS_PAGEINFO =
      JdbcMessages.EXCEPTION_NUM_OF_PAGES_ARG_OVERLAPPED_PAGES_ARG_ARG_ARG_9F985997;
  public static final String STATISTICS_RESULT_LINES =
      JdbcMessages.EXCEPTION_LINES_OF_RESULT_ARG_26F474C0;
  public static final String STATISTICS_PRC_INFO =
      JdbcMessages.EXCEPTION_NUM_OF_RPC_ARG_AVG_COST_ARG_MS_435D62C7;

  public static final String TREE = "tree";
  public static final String TABLE = "table";

  // version number
  public enum Version {
    V_0_12,
    V_0_13,
    V_1_0
  }
}
