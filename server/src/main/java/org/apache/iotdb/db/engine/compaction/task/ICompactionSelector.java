/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.compaction.task;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

import java.util.List;

/**
 * AbstractCompactionSelector is the base class of all CompactionSelector. It runs the file
 * selection process, if there still threads availabe for compaction task, it will submit a
 * compaction task to {@link org.apache.iotdb.db.engine.compaction.CompactionTaskManager} and
 * increase the global compaction task count.
 */
public interface ICompactionSelector {
  default List<AbstractCompactionTask> selectInnerSpaceTask(List<TsFileResource> resources) {
    throw new RuntimeException("This kind of selector cannot be used to select inner space task");
  }

  default List<AbstractCompactionTask> selectCrossSpaceTask(
      List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles) {
    {
      throw new RuntimeException("This kind of selector cannot be used to select cross space task");
    }
  }
}
