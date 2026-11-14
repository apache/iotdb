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

package org.apache.iotdb.consensus.raft.core.log;

public enum EntryType {
  DATA(1),
  NOOP(2),
  CONFIG(3),
  BARRIER(4);

  public final int code;

  EntryType(int code) {
    this.code = code;
  }

  public static EntryType fromCode(int code) {
    for (EntryType t : values()) {
      if (t.code == code) {
        return t;
      }
    }
    throw new IllegalArgumentException("unknown entry type " + code);
  }

  /** Entries that are not handed to the state machine. */
  public boolean isControl() {
    return this != DATA;
  }
}
