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
package org.apache.iotdb.db.engine.compaction.execute.performer;

import org.apache.iotdb.db.engine.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

import java.util.List;

/**
 * CompactionPerformer is used to compact multiple files into one or multiple files. Different
 * performers may use different implementation to achieve this goal. Some may read chunk directly
 * from tsfile, and some may using query tools to read data point by point from tsfile. Notice, not
 * all kinds of Performer can be used for all kinds of compaction tasks!
 */
public interface ICompactionPerformer {

  void perform() throws Exception;

  void setTargetFiles(List<TsFileResource> targetFiles);

  void setSummary(CompactionTaskSummary summary);

  default void setSourceFiles(List<TsFileResource> files) {
    throw new RuntimeException("Cannot set single type of source files to this kind of performer");
  }

  default void setSourceFiles(List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles) {
    throw new RuntimeException(
        "Cannot set both seq files and unseq files to this kind of performer");
  }
}
