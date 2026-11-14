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

package org.apache.iotdb.consensus.raft.core;

/**
 * Scripted crash points (mirrors the eight names of the Python reference implementation, {@code
 * node.py:_cp}). {@code FOLLOWER_POST_WRITE} is raised by the simulated log store between write and
 * sync; the others are raised by the kernel.
 */
public enum CrashPoint {
  INGEST_PRE_PERSIST,
  INGEST_MID,
  FOLLOWER_PRE_STORE,
  FOLLOWER_POST_WRITE,
  FOLLOWER_POST_FSYNC,
  FOLLOWER_POST_RESPOND,
  VOTE_PRE_PERSIST,
  VOTE_POST_PERSIST
}
