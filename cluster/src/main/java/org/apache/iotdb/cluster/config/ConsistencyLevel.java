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
package org.apache.iotdb.cluster.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum ConsistencyLevel {
  /**
   * Strong consistency means the server will first try to synchronize with the leader to get the
   * newest meta data, if failed(timeout), directly report an error to the user;
   */
  STRONG_CONSISTENCY("strong"),

  /**
   * mid consistency means the server will first try to synchronize with the leader, but if
   * failed(timeout), it will give up and just use current data it has cached before;
   */
  MID_CONSISTENCY("mid"),

  /** weak consistency do not synchronize with the leader and simply use the local data */
  WEAK_CONSISTENCY("weak"),
  ;

  private String consistencyLevelName;
  private static final Logger logger = LoggerFactory.getLogger(ConsistencyLevel.class);

  ConsistencyLevel(String consistencyLevelName) {
    this.consistencyLevelName = consistencyLevelName;
  }

  public static ConsistencyLevel getConsistencyLevel(String consistencyLevel) {
    if (consistencyLevel == null) {
      return ConsistencyLevel.MID_CONSISTENCY;
    }
    switch (consistencyLevel.toLowerCase()) {
      case "strong":
        return ConsistencyLevel.STRONG_CONSISTENCY;
      case "mid":
        return ConsistencyLevel.MID_CONSISTENCY;
      case "weak":
        return ConsistencyLevel.WEAK_CONSISTENCY;
      default:
        logger.warn(
            "Unsupported consistency level={}, use default consistency level={}",
            consistencyLevel,
            ConsistencyLevel.MID_CONSISTENCY.consistencyLevelName);
        return ConsistencyLevel.MID_CONSISTENCY;
    }
  }
}
