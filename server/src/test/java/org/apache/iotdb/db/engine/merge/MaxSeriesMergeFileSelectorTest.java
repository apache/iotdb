/**
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

import static org.junit.Assert.assertEquals;

import java.util.List;
import org.apache.iotdb.db.engine.merge.selector.MaxSeriesMergeFileSelector;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.junit.Test;

public class MaxSeriesMergeFileSelectorTest extends MergeTest{

  @Test
  public void testFullSelection() throws MergeException {
    MaxSeriesMergeFileSelector mergeFileSelector = new MaxSeriesMergeFileSelector(seqResources, unseqResources,
        Long.MAX_VALUE);
    List[] result = mergeFileSelector.select();
    List<TsFileResource> seqSelected = result[0];
    List<TsFileResource> unseqSelected = result[1];
    assertEquals(seqResources, seqSelected);
    assertEquals(unseqResources, unseqSelected);
    assertEquals(MaxSeriesMergeFileSelector.MAX_SERIES_NUM, mergeFileSelector.getConcurrentMergeNum());

    mergeFileSelector = new MaxSeriesMergeFileSelector(seqResources.subList(0, 1), unseqResources,
        Long.MAX_VALUE);
    result = mergeFileSelector.select();
    seqSelected = result[0];
    unseqSelected = result[1];
    assertEquals(seqResources.subList(0, 1), seqSelected);
    assertEquals(unseqResources, unseqSelected);
    assertEquals(MaxSeriesMergeFileSelector.MAX_SERIES_NUM, mergeFileSelector.getConcurrentMergeNum());

    mergeFileSelector = new MaxSeriesMergeFileSelector(seqResources, unseqResources.subList(0, 1),
        Long.MAX_VALUE);
    result = mergeFileSelector.select();
    seqSelected = result[0];
    unseqSelected = result[1];
    assertEquals(seqResources.subList(0, 1), seqSelected);
    assertEquals(unseqResources.subList(0, 1), unseqSelected);
    assertEquals(MaxSeriesMergeFileSelector.MAX_SERIES_NUM, mergeFileSelector.getConcurrentMergeNum());
  }

  @Test
  public void testNonSelection() throws MergeException {
    MaxSeriesMergeFileSelector mergeFileSelector = new MaxSeriesMergeFileSelector(seqResources, unseqResources,
        1);
    List[] result = mergeFileSelector.select();
    assertEquals(0, result.length);
    assertEquals(0, mergeFileSelector.getConcurrentMergeNum());
  }

  @Test
  public void testRestrictedSelection() throws MergeException {
    MaxSeriesMergeFileSelector mergeFileSelector = new MaxSeriesMergeFileSelector(seqResources, unseqResources,
        400000);
    List[] result = mergeFileSelector.select();
    List<TsFileResource> seqSelected = result[0];
    List<TsFileResource> unseqSelected = result[1];
    assertEquals(seqResources.subList(0, 2), seqSelected);
    assertEquals(unseqResources.subList(0, 2), unseqSelected);
    assertEquals(MaxSeriesMergeFileSelector.MAX_SERIES_NUM, mergeFileSelector.getConcurrentMergeNum());
  }

  @Test
  public void testRestrictedSelection2() throws MergeException {
    MaxSeriesMergeFileSelector mergeFileSelector = new MaxSeriesMergeFileSelector(seqResources, unseqResources,
        70000);
    List[] result = mergeFileSelector.select();
    List<TsFileResource> seqSelected = result[0];
    List<TsFileResource> unseqSelected = result[1];
    assertEquals(seqResources.subList(0, 1), seqSelected);
    assertEquals(unseqResources.subList(0, 1), unseqSelected);
    assertEquals(54, mergeFileSelector.getConcurrentMergeNum());
  }
}
