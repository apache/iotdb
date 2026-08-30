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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.load;

/**
 * Consensus LOAD request phases, sequenced by the scheduler/client.
 *
 * <p>{@link #BEGIN} opens a load, {@link #PIECE} stages one data piece, {@link #PREPARE} seals the
 * data of a load, {@link #COMMIT} imports the staged data, {@link #ABORT} discards it, and {@link
 * #PULL} asks the current write node to re-deliver the serialized bytes of one already-applied
 * piece so a follower that did not receive the data yet can catch up. Phase enforcement and
 * idempotency are the client's responsibility; the server applies each request against the staged
 * data directory identified by the load id without keeping per-load transaction state in memory.
 */
public enum LoadTsFileConsensusOp {
  BEGIN,
  PIECE,
  PREPARE,
  COMMIT,
  ABORT,
  PULL;

  public static LoadTsFileConsensusOp fromOrdinal(int ordinal) {
    final LoadTsFileConsensusOp[] values = values();
    if (ordinal < 0 || ordinal >= values.length) {
      throw new IllegalArgumentException(
          org.apache.iotdb.db.i18n.DataNodeQueryMessages
                  .EXCEPTION_UNKNOWN_LOADTSFILECONSENSUSOP_ORDINAL_ARG_62848FC2
              + ordinal);
    }
    return values[ordinal];
  }
}
