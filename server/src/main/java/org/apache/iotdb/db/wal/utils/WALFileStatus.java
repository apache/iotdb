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
package org.apache.iotdb.db.wal.utils;

/**
 * This enumeration class annotates the status of wal file. This first bit is used to denote whether
 * this contains search index.
 */
public enum WALFileStatus {
  /** This file doesn't contain content needs searching */
  CONTAINS_NONE_SEARCH_INDEX(0),
  /** This file contains content needs searching */
  CONTAINS_SEARCH_INDEX(1),
  ;

  private final int code;

  WALFileStatus(int code) {
    this.code = code;
  }

  public int getCode() {
    return code;
  }

  public static WALFileStatus valueOf(int code) {
    for (WALFileStatus status : WALFileStatus.values()) {
      if (status.code == code) {
        return status;
      }
    }
    return null;
  }
}
