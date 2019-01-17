/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.writelog;

public enum RecoverStage {
  /**
   * This is just the start point of the recovery auto mata.
   */
  init,

  /**
   * In this stage, the mission is to backup restore file, processor.store file with suffix
   * "-recovery". Should SET flag afterward.
   */
  backup,

  /**
   * In this stage, the mission is to recover TsFile / OverflowFile with restore file Should NOT SET
   * flag afterward.
   */
  recoverFile,

  /**
   * In this stage, the mission is to read logs from wal and wal-old files (if exists) and replay
   * them. Should SET flag afterward,
   */
  replayLog,

  /**
   * In this stage, the mission is to clean all "-recovery" files, log file and recovery flag.
   * Should CLEAN flag afterward.
   */
  cleanup
}
