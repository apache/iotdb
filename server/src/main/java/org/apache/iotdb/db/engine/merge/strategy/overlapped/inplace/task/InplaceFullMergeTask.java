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

package org.apache.iotdb.db.engine.merge.strategy.overlapped.inplace.task;

import org.apache.iotdb.db.engine.merge.MergeCallback;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;

/**
 * MergeTask merges given seqFiles and unseqFiles into new ones, which basically consists of three
 * steps: 1. rewrite overflowed, modified or small-sized chunks into temp merge files 2. move the
 * merged chunks in the temp files back to the seqFiles or move the unmerged chunks in the seqFiles
 * into temp files and replace the seqFiles with the temp files. 3. remove unseqFiles
 */
public class InplaceFullMergeTask extends InplaceBaseMergeTask {

  public InplaceFullMergeTask(MergeResource mergeResource, String storageGroupSysDir,
      MergeCallback callback, String taskName, String storageGroupName) {
    super(mergeResource, storageGroupSysDir, callback, true, taskName, storageGroupName);
  }

}
