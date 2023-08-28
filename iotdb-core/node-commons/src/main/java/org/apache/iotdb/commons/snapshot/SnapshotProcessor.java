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

package org.apache.iotdb.commons.snapshot;

import org.apache.thrift.TException;

import java.io.File;
import java.io.IOException;

/**
 * Different from interface of state machines interacting with consensus groups. This interface is
 * where each module actually performs the snapshot inside the state machine. Every module that can
 * actually process(modify) snapshot content should implement it.
 */
public interface SnapshotProcessor {

  /**
   * Take snapshot
   *
   * @param snapshotDir Where snapshot files are stored.
   * @return Whether the snapshot is successfully executed
   * @throws TException Exception occurred during the thrift serialize struct
   * @throws IOException Exception related to file read and write
   */
  boolean processTakeSnapshot(File snapshotDir) throws TException, IOException;

  /**
   * Load snapshot
   *
   * @param snapshotDir Load snapshot from here
   * @throws TException Exception occurred during the thrift deserialize struct
   * @throws IOException Exception related to file read and write
   */
  void processLoadSnapshot(File snapshotDir) throws TException, IOException;
}
