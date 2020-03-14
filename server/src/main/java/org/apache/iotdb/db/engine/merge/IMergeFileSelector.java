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

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;

/**
 * IMergeFileSelector selects a set of files from given seqFiles and unseqFiles which can be
 * merged without exceeding given memory budget.
 */
public interface IMergeFileSelector {

  /**
   * Select merge candidates from seqFiles and unseqFiles under the given memoryBudget.
   * This process iteratively adds the next unseqFile from unseqFiles and its overlapping seqFiles
   * as newly-added candidates and computes their estimated memory cost. If the current cost
   * pluses the new cost is still under the budget, accept the unseqFile and the seqFiles as
   * candidates, otherwise go to the next iteration.
   * The memory cost of a file is calculated in two ways:
   *    The rough estimation: for a seqFile, the size of its metadata is used for estimation.
   *    Since in the worst case, the file only contains one timeseries and all its metadata will
   *    be loaded into memory with at most one actual data chunk (which is negligible) and writing
   *    the timeseries into a new file generate metadata of the similar size, so the size of all
   *    seqFiles' metadata (generated when writing new chunks) pluses the largest one (loaded
   *    when reading a timeseries from the seqFiles) is the total estimation of all seqFiles; for
   *    an unseqFile, since the merge reader may read all chunks of a series to perform a merge
   *    read, the whole file may be loaded into memory, so we use the file's length as the
   *    maximum estimation.
   *    The tight estimation: based on the rough estimation, we scan the file's metadata to
   *    count the number of chunks for each series, find the series which have the most
   *    chunks in the file and use its chunk proportion to refine the rough estimation.
   * The rough estimation is performed first, if no candidates can be found using rough
   * estimation, we run the selection again with tight estimation.
   * @return two lists of TsFileResource, the former is selected seqFiles and the latter is
   * selected unseqFiles or an empty array if there are no proper candidates by the budget.
   * @throws MergeException
   */
  void select() throws MergeException;

  void select(boolean useTightBound) throws MergeException, IOException;

  int getConcurrentMergeNum();

  void setConcurrentMergeNum(int concurrentMergeNum);

  MergeResource getResource();

  List<TsFileResource> getSelectedSeqFiles();

  List<TsFileResource> getSelectedUnseqFiles();

  long getTotalCost();
}
