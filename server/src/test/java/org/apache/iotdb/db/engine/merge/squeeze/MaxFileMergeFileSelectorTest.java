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

package org.apache.iotdb.db.engine.merge.squeeze;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.engine.merge.IMergeFileSelector;
import org.apache.iotdb.db.engine.merge.MergeTest;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.squeeze.selector.SqueezeMaxFileSelector;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.junit.Test;

public class MaxFileMergeFileSelectorTest extends MergeTest {

  @Test
  public void testFullSelection() throws MergeException, IOException {
    MergeResource resource = new MergeResource(seqResources, unseqResources);
    IMergeFileSelector mergeFileSelector = new SqueezeMaxFileSelector(resource, Long.MAX_VALUE);
    mergeFileSelector.select();
    List<TsFileResource> seqSelected = mergeFileSelector.getSelectedSeqFiles();
    List<TsFileResource> unseqSelected = mergeFileSelector.getSelectedUnseqFiles();
    assertEquals(seqResources, seqSelected);
    assertEquals(unseqResources, unseqSelected);
    resource.clear();

    resource = new MergeResource(seqResources.subList(0, 1), unseqResources);
    mergeFileSelector = new SqueezeMaxFileSelector(resource, Long.MAX_VALUE);
    mergeFileSelector.select();
    seqSelected = mergeFileSelector.getSelectedSeqFiles();
    unseqSelected = mergeFileSelector.getSelectedUnseqFiles();
    assertEquals(seqResources.subList(0, 1), seqSelected);
    assertEquals(unseqResources, unseqSelected);
    resource.clear();

    resource = new MergeResource(seqResources, unseqResources.subList(0, 1));
    mergeFileSelector = new SqueezeMaxFileSelector(resource, Long.MAX_VALUE);
    mergeFileSelector.select();
    seqSelected = mergeFileSelector.getSelectedSeqFiles();
    unseqSelected = mergeFileSelector.getSelectedUnseqFiles();
    assertEquals(seqResources.subList(0, 5), seqSelected);
    assertEquals(unseqResources.subList(0, 1), unseqSelected);
    resource.clear();
  }

  @Test
  public void testNonSelection() throws MergeException, IOException {
    MergeResource resource = new MergeResource(seqResources, unseqResources);
    IMergeFileSelector mergeFileSelector = new SqueezeMaxFileSelector(resource, 1);
    mergeFileSelector.select();
    assertTrue(mergeFileSelector.getSelectedSeqFiles().isEmpty());
    assertTrue(mergeFileSelector.getSelectedUnseqFiles().isEmpty());
    resource.clear();
  }

  @Test
  public void testRestrictedSelection() throws MergeException, IOException {
    MergeResource resource = new MergeResource(seqResources, unseqResources);
    IMergeFileSelector mergeFileSelector = new SqueezeMaxFileSelector(resource, 400000);
    mergeFileSelector.select();
    List<TsFileResource> seqSelected = mergeFileSelector.getSelectedSeqFiles();
    List<TsFileResource> unseqSelected = mergeFileSelector.getSelectedUnseqFiles();
    assertEquals(seqResources.subList(0, 4), seqSelected);
    assertEquals(unseqResources.subList(0, 2), unseqSelected);
    resource.clear();
  }
}
