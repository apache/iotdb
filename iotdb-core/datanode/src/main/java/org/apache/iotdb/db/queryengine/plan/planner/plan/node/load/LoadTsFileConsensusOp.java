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

import org.apache.iotdb.db.i18n.DataNodeQueryMessages;

/**
 * The consensus commands of one LOAD transaction: {@link #PIECE} stages a piece, {@link #PREPARE}
 * seals the staged data once every piece arrived, {@link #COMMIT} imports it into the region and
 * {@link #ABORT} discards it. There is no "begin" phase - the first PIECE creates the staging
 * directory of its region.
 *
 * <p>The codes go on the wire, into the WAL and the progress records, so they are never renumbered.
 */
public enum LoadTsFileConsensusOp {
  PIECE(1),
  PREPARE(2),
  COMMIT(3),
  ABORT(4);

  private static final LoadTsFileConsensusOp[] VALUES = values();

  private final int code;

  LoadTsFileConsensusOp(final int code) {
    this.code = code;
  }

  public int getCode() {
    return code;
  }

  public static LoadTsFileConsensusOp fromCode(final int code) {
    for (final LoadTsFileConsensusOp op : VALUES) {
      if (op.code == code) {
        return op;
      }
    }
    throw new IllegalArgumentException(
        String.format(
            DataNodeQueryMessages.EXCEPTION_UNKNOWN_LOADTSFILECONSENSUSOP_CODE_ARG_B62D2997, code));
  }
}
