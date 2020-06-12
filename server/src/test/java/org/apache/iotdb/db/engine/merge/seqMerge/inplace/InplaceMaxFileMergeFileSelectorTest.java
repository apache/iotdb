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

package org.apache.iotdb.db.engine.merge.seqMerge.inplace;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.engine.merge.IMergeFileSelector;
import org.apache.iotdb.db.engine.merge.MergeTest;
import org.apache.iotdb.db.engine.merge.seqMerge.inplace.selector.InplaceMaxFileSelector;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.utils.SelectorContext;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.tsfile.utils.Pair;
import org.junit.Test;

public class InplaceMaxFileMergeFileSelectorTest extends MergeTest {

  @Test
  public void testFullSelection() throws MergeException, IOException {
    IMergeFileSelector mergeFileSelector = new InplaceMaxFileSelector(seqResources,
        unseqResources, Long.MAX_VALUE);
    Pair<MergeResource, SelectorContext> selectRes = mergeFileSelector.selectMergedFiles();
    MergeResource mergeResource = selectRes.left;
    List<TsFileResource> seqSelected = mergeResource.getSeqFiles();
    List<TsFileResource> unseqSelected = mergeResource.getUnseqFiles();
    assertEquals(seqResources, seqSelected);
    assertEquals(unseqResources, unseqSelected);
    mergeResource.clear();

    mergeFileSelector = new InplaceMaxFileSelector(seqResources.subList(0, 1),
        unseqResources, Long.MAX_VALUE);
    selectRes = mergeFileSelector.selectMergedFiles();
    mergeResource = selectRes.left;
    seqSelected = mergeResource.getSeqFiles();
    unseqSelected = mergeResource.getUnseqFiles();
    assertEquals(seqResources.subList(0, 1), seqSelected);
    assertEquals(unseqResources, unseqSelected);
    mergeResource.clear();

    mergeFileSelector = new InplaceMaxFileSelector(seqResources,
        unseqResources.subList(0, 1), Long.MAX_VALUE);
    selectRes = mergeFileSelector.selectMergedFiles();
    mergeResource = selectRes.left;
    seqSelected = mergeResource.getSeqFiles();
    unseqSelected = mergeResource.getUnseqFiles();
    assertEquals(seqResources.subList(0, 1), seqSelected);
    assertEquals(unseqResources.subList(0, 1), unseqSelected);
    mergeResource.clear();
  }

  @Test
  public void testNonSelection() throws MergeException, IOException {
    IMergeFileSelector mergeFileSelector = new InplaceMaxFileSelector(seqResources,
        unseqResources, 1);
    Pair<MergeResource, SelectorContext> selectRes = mergeFileSelector.selectMergedFiles();
    MergeResource mergeResource = selectRes.left;
    assertEquals(0, mergeResource.getUnseqFiles().size());
    assertEquals(0, mergeResource.getSeqFiles().size());
    mergeResource.clear();
  }

  @Test
  public void testRestrictedSelection() throws MergeException, IOException {
    IMergeFileSelector mergeFileSelector = new InplaceMaxFileSelector(seqResources,
        unseqResources, 400000);
    Pair<MergeResource, SelectorContext> selectRes = mergeFileSelector.selectMergedFiles();
    MergeResource mergeResource = selectRes.left;
    List<TsFileResource> seqSelected = mergeResource.getSeqFiles();
    List<TsFileResource> unseqSelected = mergeResource.getUnseqFiles();
    assertEquals(seqResources.subList(0, 3), seqSelected);
    assertEquals(unseqResources.subList(0, 3), unseqSelected);
    mergeResource.clear();
  }
}
