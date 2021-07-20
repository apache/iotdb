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

package org.apache.iotdb.db.engine.compaction.newcross.fragment.task;

import org.apache.iotdb.db.engine.compaction.newcross.AbstractCrossSpaceCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

import java.util.List;
import java.util.Map;

/**
 * This class implement the FragmentCompaction, which compact the overlapped data in unsequence file
 * to sequence file. Notice, in FragmentCompaction, <b>only the overlapped data segment will be
 * compacted</b>! The non-overlapping data segment will stay in the unsequence space. E.g. Before
 * compaction:
 *
 * <ul>
 *   <li>Time range of sequence files: 1-10 15-20
 *   <li>Time range of unsequence files: 7-23
 * </ul>
 *
 * After compaction:
 *
 * <ul>
 *   <li>Time range of sequence files: 1-10 15-20
 *   <li>Time range of unsequence files: 11-14 21-23
 * </ul>
 *
 * <p>If the data segment remained in unsequence space is continuous, e.g. 11-20, an .mods file will
 * be added to this unsequence file and the .resource file will be edited. Else if data segment
 * remained is discontinuous, e.g. 11-20 and 25-27, the unsequence file will be split into two file.
 * Even if there are more than two discontinuous data segments, the unsequence file will still only
 * be split into two files, the end time of first one is smaller than the start time of sequence
 * file, and the start time of the other is greater than sequence file.
 */
public class FragmentCompactionTask extends AbstractCrossSpaceCompactionTask {

  private Map<TsFileResource, List<TsFileResource>> selectedFiles;

  public FragmentCompactionTask(
      Map<TsFileResource, List<TsFileResource>> selectedFiles,
      String fullStorageGroupName,
      long timePartition) {
    super(fullStorageGroupName, timePartition);
    this.selectedFiles = selectedFiles;
  }

  @Override
  protected void doCompaction() throws Exception {}
}
