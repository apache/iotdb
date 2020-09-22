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

package org.apache.iotdb.db.engine.merge;

import java.util.concurrent.Callable;
import org.apache.iotdb.db.engine.merge.manage.MergeContext;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;

public abstract class MergeTask implements Callable<Void> {
  protected MergeResource resource;
  protected String storageGroupSysDir;
  protected String storageGroupName;
  protected MergeLogger mergeLogger;
  protected MergeContext mergeContext = new MergeContext();

  protected MergeCallback callback;
  protected String taskName;
  protected boolean fullMerge;
  protected States states = States.START;

  public MergeTask(MergeResource mergeResource, String storageGroupSysDir,
      MergeCallback callback, String taskName, boolean fullMerge, String storageGroupName) {
    this.resource = mergeResource;
    this.storageGroupSysDir = storageGroupSysDir;
    this.callback = callback;
    this.taskName = taskName;
    this.fullMerge = fullMerge;
    this.storageGroupName = storageGroupName;
  }

  public String getStorageGroupName() {
    return storageGroupName;
  }

  protected enum States {
    START,
    MERGE_CHUNKS,
    MERGE_FILES,
    CLEAN_UP,
    ABORTED
  }

  public String getProgress() {
    switch (states) {
      case ABORTED:
        return "Aborted";
      case CLEAN_UP:
        return "Cleaning up";
      case MERGE_FILES:
        return "Merging files";
      case MERGE_CHUNKS:
        return "Merging series";
      case START:
      default:
        return "Just started";
    }
  }

  public String getTaskName() {
    return taskName;
  }
}
