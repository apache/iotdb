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
package org.apache.iotdb.db.engine.compaction.cross;

import org.apache.iotdb.db.engine.compaction.constant.CrossCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.CrossSpaceCompactionResource;
import org.apache.iotdb.db.engine.compaction.cross.utils.ICompactionEstimator;
import org.apache.iotdb.db.engine.compaction.cross.utils.RewriteCompactionEstimator;
import org.apache.iotdb.db.engine.compaction.task.ICompactionSelector;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.List;

public interface ICrossSpaceSelector extends ICompactionSelector {
  List<Pair<List<TsFileResource>, List<TsFileResource>>> selectCrossSpaceTask(
      List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles);

  static ICompactionEstimator getCompactionEstimator(
      CrossCompactionPerformer compactionPerformer, CrossSpaceCompactionResource resource) {
    switch (compactionPerformer) {
      case READ_POINT:
      default:
        return new RewriteCompactionEstimator(resource);
    }
  }
}
