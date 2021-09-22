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
package org.apache.iotdb.db.query.control.tracing;

/** this class contains several constants used in Tracing. */
public class TracingConstant {

  private TracingConstant() {}

  public static final String STATEMENT_ID = "Statement Id: %d";
  public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";

  public static final String ACTIVITY_START_EXECUTE = "Start to execute statement: %s";
  public static final String ACTIVITY_PARSE_SQL = "Parse SQL to physical plan";
  public static final String ACTIVITY_CREATE_DATASET = "Create and cache dataset";
  public static final String ACTIVITY_REQUEST_COMPLETE = "Request complete";

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

  public static final long TIME_NULL = -1L;
}
