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

package org.apache.iotdb.db.engine.storagegroup;

import org.apache.iotdb.db.constant.TestConstant;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TsFileResourceListTest {

  private TsFileResource generateTsFileResource(int id) {
    File file =
        new File(
            TsFileNameGenerator.generateNewTsFilePath(
                TestConstant.BASE_OUTPUT_PATH, id, id, id, id));
    return new TsFileResource(file);
  }

  @Test
  public void testAdd() {
    TsFileResourceList tsFileResourceList = new TsFileResourceList();
    List<TsFileResource> tsFileResources = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      TsFileResource resource = generateTsFileResource(i);
      tsFileResources.add(resource);
      tsFileResourceList.add(resource);
    }
    TsFileResource resourceHasNext = new TsFileResource();
    resourceHasNext.next = resourceHasNext;
    Assert.assertFalse(tsFileResourceList.add(resourceHasNext));

    TsFileResource resourceHasPre = new TsFileResource();
    resourceHasPre.prev = resourceHasPre;
    Assert.assertFalse(tsFileResourceList.add(resourceHasPre));

    Assert.assertEquals(5, tsFileResourceList.size());
    Iterator<TsFileResource> iterator = tsFileResourceList.iterator();
    int index = 0;
    while (iterator.hasNext()) {
      Assert.assertSame(tsFileResources.get(index++), iterator.next());
    }
  }

  @Test
  public void testRemove() {
    TsFileResourceList tsFileResourceList = new TsFileResourceList();
    List<TsFileResource> tsFileResources = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      TsFileResource resource = generateTsFileResource(i);
      tsFileResources.add(resource);
      tsFileResourceList.add(resource);
    }

    // remove the first resource
    tsFileResourceList.remove(tsFileResources.get(0));
    tsFileResources.remove(0);
    Assert.assertEquals(4, tsFileResourceList.size());

    // remove the last resource
    TsFileResource toBeRemoved = tsFileResources.get(3);
    tsFileResourceList.remove(toBeRemoved);
    tsFileResources.remove(toBeRemoved);

    // compare each resource
    Iterator<TsFileResource> iterator = tsFileResourceList.iterator();
    int index = 0;
    while (iterator.hasNext()) {
      Assert.assertSame(tsFileResources.get(index++), iterator.next());
    }
  }

  @Test
  public void removeNotIncludedResourceTest() {
    TsFileResourceList tsFileResourceList = new TsFileResourceList();
    TsFileResource resource = new TsFileResource();
    tsFileResourceList.add(resource);

    tsFileResourceList.remove(resource);
    Assert.assertEquals(0, tsFileResourceList.size());

    TsFileResource notIncluded = new TsFileResource();
    Assert.assertFalse(tsFileResourceList.remove(notIncluded));
  }
}
