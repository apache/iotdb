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

package org.apache.iotdb.commons.pipe.agent.task.meta;

public enum PipeType {
  USER((byte) 0),
  SUBSCRIPTION((byte) 1),
  CONSENSUS((byte) 2),
  ;

  private final byte type;

  PipeType(byte type) {
    this.type = type;
  }

  public static PipeType getPipeType(String pipeName) {
    if (pipeName.startsWith(PipeStaticMeta.SUBSCRIPTION_PIPE_PREFIX)) {
      return SUBSCRIPTION;
    } else if (pipeName.startsWith(PipeStaticMeta.CONSENSUS_PIPE_PREFIX)) {
      return CONSENSUS;
    }
    return USER;
  }
}
