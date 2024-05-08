/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.index.common;

public class IndexConstant {

  // whole matching
  public static final int NON_SET_TOP_K = -1;
  public static final String TOP_K = "TOP_K";

  public static final String PATTERN = "PATTERN";
  public static final String THRESHOLD = "THRESHOLD";

  // RTree PAA parameters
  public static final String PAA_DIM = "PAA_DIM";

  // ELB: calc param
  public static final String BLOCK_SIZE = "BLOCK_SIZE";

  private IndexConstant() {}
}
