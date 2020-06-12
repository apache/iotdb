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

package org.apache.iotdb.db.engine.merge.seqMerge.squeeze;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.engine.merge.IMergeFileSelector;
import org.apache.iotdb.db.engine.merge.MergeTest;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.seqMerge.squeeze.selector.SqueezeMaxFileSelector;
import org.apache.iotdb.db.engine.merge.utils.SelectorContext;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.tsfile.utils.Pair;
import org.junit.Test;

public class SqueezeMaxFileMergeFileSelectorTest extends MergeTest {

  @Test
  public void testFullSelection() throws MergeException, IOException {
    IMergeFileSelector mergeFileSelector = new SqueezeMaxFileSelector(seqResources,
        unseqResources.subList(0, 1),
        Long.MAX_VALUE);
    Pair<MergeResource, SelectorContext> selectRes = mergeFileSelector.selectMergedFiles();
    MergeResource mergeResource = selectRes.left;
    List<TsFileResource> seqSelected = mergeResource.getSeqFiles();
    List<TsFileResource> unseqSelected = mergeResource.getUnseqFiles();
    assertEquals(seqResources.subList(0, 5), seqSelected);
    assertEquals(unseqResources.subList(0, 1), unseqSelected);
    mergeResource.clear();
  }

  @Test
  public void testNonSelection() throws MergeException, IOException {
    IMergeFileSelector mergeFileSelector = new SqueezeMaxFileSelector(seqResources, unseqResources,
        1);
    Pair<MergeResource, SelectorContext> selectRes = mergeFileSelector.selectMergedFiles();
    MergeResource mergeResource = selectRes.left;
    assertTrue(mergeResource.getSeqFiles().isEmpty());
    assertTrue(mergeResource.getUnseqFiles().isEmpty());
    mergeResource.clear();
  }

  @Test
  public void testRestrictedSelection() throws MergeException, IOException {
    IMergeFileSelector mergeFileSelector = new SqueezeMaxFileSelector(seqResources, unseqResources,
        400000);
    Pair<MergeResource, SelectorContext> selectRes = mergeFileSelector.selectMergedFiles();
    MergeResource mergeResource = selectRes.left;
    List<TsFileResource> seqSelected = mergeResource.getSeqFiles();
    List<TsFileResource> unseqSelected = mergeResource.getUnseqFiles();
    assertEquals(seqResources.subList(0, 5), seqSelected);
    assertEquals(unseqResources.subList(0, 3), unseqSelected);
    mergeResource.clear();
  }
}
