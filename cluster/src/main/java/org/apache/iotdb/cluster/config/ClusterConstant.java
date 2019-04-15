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
package org.apache.iotdb.cluster.config;

public class ClusterConstant {

  private ClusterConstant(){

  }

  public static final String SET_READ_METADATA_CONSISTENCY_LEVEL_PATTERN = "set\\s+read\\s+metadata\\s+level\\s+to\\s+\\d+";
  public static final String SET_READ_DATA_CONSISTENCY_LEVEL_PATTERN = "set\\s+read\\s+data\\s+level\\s+to\\s+\\d+";
  public static final int MAX_CONSISTENCY_LEVEL = 2;
  public static final int STRONG_CONSISTENCY_LEVEL = 1;
  public static final int WEAK_CONSISTENCY_LEVEL = 2;

  /**
   * Maximum time of blocking main thread for waiting for all running task threads and tasks in the
   * queue until end. Each client request corresponds to a QP Task. A QP task may be divided into
   * several sub-tasks.The unit is milliseconds.
   */
  public static final int CLOSE_QP_SUB_TASK_BLOCK_TIMEOUT = 1000;

}
