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
package org.apache.iotdb.db.sync.sender.manager;

import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.sync.sender.pipe.TsFilePipe;

import java.io.File;
import java.util.List;

/**
 * ISyncManager is designed for collect all history TsFiles(i.e. before the pipe start time, all
 * tsfiles whose memtable is set to null.) and realtime TsFiles for registered {@link TsFilePipe}.
 */
public interface ISyncManager {
  /** tsfile */
  void syncRealTimeDeletion(Deletion deletion);

  void syncRealTimeTsFile(File tsFile);

  void syncRealTimeResource(File tsFile);

  List<File> syncHistoryTsFile(long dataStartTime);

  File createHardlink(File tsFile, long modsOffset);

  void delete();
}
