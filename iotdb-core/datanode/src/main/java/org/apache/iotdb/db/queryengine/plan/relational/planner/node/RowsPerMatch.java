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

package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public enum RowsPerMatch {
  ONE {
    @Override
    public boolean isOneRow() {
      return true;
    }

    @Override
    public boolean isEmptyMatches() {
      return true;
    }

    @Override
    public boolean isUnmatchedRows() {
      return false;
    }
  },

  // ALL_SHOW_EMPTY option applies to the MATCH_RECOGNIZE clause.
  // Output all rows of every match, including empty matches.
  // In the case of an empty match, output the starting row of the match attempt.
  // Do not produce output for the rows matched within exclusion `{- ... -}`.
  ALL_SHOW_EMPTY {
    @Override
    public boolean isOneRow() {
      return false;
    }

    @Override
    public boolean isEmptyMatches() {
      return true;
    }

    @Override
    public boolean isUnmatchedRows() {
      return false;
    }
  },

  // ALL_OMIT_EMPTY option applies to the MATCH_RECOGNIZE clause.
  // Output all rows of every non-empty match.
  // Do not produce output for the rows matched within exclusion `{- ... -}`
  ALL_OMIT_EMPTY {
    @Override
    public boolean isOneRow() {
      return false;
    }

    @Override
    public boolean isEmptyMatches() {
      return false;
    }

    @Override
    public boolean isUnmatchedRows() {
      return false;
    }
  },

  // ALL_WITH_UNMATCHED option applies to the MATCH_RECOGNIZE clause.
  // Output all rows of every match, including empty matches.
  // Produce an additional output row for every unmatched row.
  // Pattern exclusions are not allowed with this option.
  ALL_WITH_UNMATCHED {
    @Override
    public boolean isOneRow() {
      return false;
    }

    @Override
    public boolean isEmptyMatches() {
      return true;
    }

    @Override
    public boolean isUnmatchedRows() {
      return true;
    }
  };

  public abstract boolean isOneRow();

  public abstract boolean isEmptyMatches();

  public abstract boolean isUnmatchedRows();

  public static void serialize(RowsPerMatch rowsPerMatch, ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(rowsPerMatch.ordinal(), byteBuffer);
  }

  public static void serialize(RowsPerMatch rowsPerMatch, DataOutputStream stream)
      throws IOException {
    ReadWriteIOUtils.write(rowsPerMatch.ordinal(), stream);
  }

  public static RowsPerMatch deserialize(ByteBuffer byteBuffer) {
    int ordinal = ReadWriteIOUtils.readInt(byteBuffer);
    return RowsPerMatch.values()[ordinal];
  }
}
