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

package org.apache.iotdb.db.storageengine.dataregion.tsfile;

public enum TsFileRepairStatus {
  NORMAL,
  NEED_TO_CHECK,
  NEED_TO_REPAIR_BY_REWRITE,
  NEED_TO_REPAIR_BY_MOVE,
  CAN_NOT_REPAIR;

  public boolean isNormalCompactionCandidate() {
    return this == NORMAL;
  }

  public boolean isRepairCompactionCandidate() {
    return this == NEED_TO_CHECK
        || this == NEED_TO_REPAIR_BY_REWRITE
        || this == NEED_TO_REPAIR_BY_MOVE;
  }
}
