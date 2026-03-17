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

package org.apache.iotdb.confignode.procedure.state.consistency;

/** State machine states for the RepairRegionProcedure. */
public enum RepairState {
  INIT,
  CHECK_SYNC_LAG,
  COMPUTE_WATERMARK,
  BUILD_MERKLE_VIEW,
  COMPARE_ROOT_HASH,
  DRILL_DOWN,
  SMALL_TSFILE_SHORT_CIRCUIT,
  NEGOTIATE_KEY_MAPPING,
  ESTIMATE_DIFF,
  EXCHANGE_IBF,
  DECODE_DIFF,
  ATTRIBUTE_DIFFS,
  SELECT_REPAIR_STRATEGY,
  EXECUTE_TSFILE_TRANSFER,
  EXECUTE_POINT_STREAMING,
  VERIFY_REPAIR,
  COMMIT_PARTITION,
  ADVANCE_WATERMARK,
  ROLLBACK,
  DONE
}
