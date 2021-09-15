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
package org.apache.iotdb.db.sync.sender.recover;

import java.io.IOException;
import java.util.Set;

/**
 * This interface is used to restore and clean up the status of the historical synchronization task
 * with abnormal termination. Through the analysis of the synchronization task log, the completed
 * progress is merged to prepare for the next synchronization task.
 */
public interface ISyncSenderLogAnalyzer {

  /**
   * Recover sync tasks that were not completed properly last time, and clean up the environment.
   */
  void recover() throws IOException;

  /** Load last local files from last local info file. */
  void loadLastLocalFiles(Set<String> lastLocalFiles);

  /**
   * Load the sync log, which indicates the progress of the last synchronization task. Deleted files
   * and new tsfiles can be obtained by log analysis.
   */
  void loadLogger(Set<String> deletedFiles, Set<String> newFiles);

  /** Update the last local info file based on the log information of the last task */
  void updateLastLocalFile(Set<String> currentLocalFiles) throws IOException;
}
