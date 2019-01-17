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
package org.apache.iotdb.db.writelog.node;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.exception.RecoverException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.writelog.LogPosition;

public interface WriteLogNode {

  /**
   * Write a log which implements LogSerializable. First, the log will be conveyed to byte[] by
   * codec. Then the byte[] will be put into a cache. If necessary, the logs in the cache will be
   * synced to disk.
   *
   * @param plan -plan
   * @return The position to be written of the log.
   */
  LogPosition write(PhysicalPlan plan) throws IOException;

  /**
   * First judge the stage of recovery by status of files, and then recover from that stage.
   */
  void recover() throws RecoverException;

  /**
   * Sync and close streams.
   */
  void close() throws IOException;

  /**
   * Write what in cache to disk.
   */
  void forceSync() throws IOException;

  /**
   * When a FileNode attempts to start a flush, this method must be called to rename log file.
   */
  void notifyStartFlush() throws IOException;

  /**
   * When the flush of a FlieNode ends, this method must be called to check if log file needs
   * cleaning.
   */
  void notifyEndFlush(List<LogPosition> logPositions);

  /**
   * return identifier of the log node.
   *
   * @return The identifier of this log node.
   */
  String getIdentifier();

  /**
   * return the directory where wal file is placed.
   *
   * @return The directory where wal file is placed.
   */
  String getLogDirectory();

  /**
   * Abandon all logs in this node and delete the log directory. The caller should guarantee that NO
   * MORE WRITE is coming.
   */
  void delete() throws IOException;
}
