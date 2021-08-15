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
package org.apache.iotdb.db.sync.receiver.load;

import org.apache.iotdb.db.sync.receiver.load.FileLoader.LoadTask;

import java.io.File;
import java.io.IOException;

/**
 * This interface is used to load files, including deleted files and new tsfiles. The
 * producer-consumer model is used to load files. A background consumer thread is used to load
 * files. There is a queue recording tasks. After receiving a file, the receiver adds a task to the
 * queue. When all files are loaded and the synchronization task is completed, the thread is closed.
 */
public interface IFileLoader {

  /** Add a deleted file name to be loaded. */
  void addDeletedFileName(File deletedFile);

  /** Add a new tsfile to be loaded. */
  void addTsfile(File tsfile);

  /** Mark sync end. */
  void endSync();

  /** Handle load task by type. */
  void handleLoadTask(LoadTask task) throws IOException;

  /** Set current load type */
  void setCurType(LoadType curType);

  void cleanUp();
}
