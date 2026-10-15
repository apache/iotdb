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
package org.apache.iotdb.db.storageengine.load;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.consensus.pipe.IoTConsensusV2;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;

import java.util.concurrent.atomic.AtomicReference;

/**
 * What the consensus protocol of this node lets a LOAD task rely on. IoTConsensusV1 replicates the
 * WAL and keeps a staged directory until its safe deletion watermark passes the command that ended
 * the task; IoTConsensusV2 replicates through pipe instead, so there is no index to wait for, and
 * it never leaves a hole behind, because a piece that did not arrive is a failure rather than
 * something a later piece may fill in.
 */
final class LoadConsensusCompat {

  private static final AtomicReference<Boolean> IOT_CONSENSUS_V2_FOR_TEST = new AtomicReference<>();

  private LoadConsensusCompat() {}

  static boolean usesIoTConsensusV2() {
    final Boolean forced = IOT_CONSENSUS_V2_FOR_TEST.get();
    return forced != null
        ? forced
        : DataRegionConsensusImpl.getInstance() instanceof IoTConsensusV2;
  }

  @TestOnly
  static void setIoTConsensusV2ForTest(final Boolean value) {
    IOT_CONSENSUS_V2_FOR_TEST.set(value);
  }
}
