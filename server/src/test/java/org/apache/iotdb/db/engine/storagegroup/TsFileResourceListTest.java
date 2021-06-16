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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.constant.TestConstant;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class TsFileResourceListTest {

  private TsFileResourceList tsFileResourceList;
  private List<TsFileResource> tsFileResources;

  private TsFileResource generateTsFileResource(int fileNum) {
    File file = new File(TestConstant.BASE_OUTPUT_PATH.concat(
        0
            + IoTDBConstant.FILE_NAME_SEPARATOR
            + fileNum
            + IoTDBConstant.FILE_NAME_SEPARATOR
            + 0
            + IoTDBConstant.FILE_NAME_SEPARATOR
            + 0
            + ".tsfile"));
    TsFileResource tsFileResource = new TsFileResource(file);
    tsFileResources.add(tsFileResource);
    return tsFileResource;
  }

  @Before
  public void setUp() {
    tsFileResourceList = new TsFileResourceList();
    tsFileResources = new ArrayList<>();
    tsFileResourceList.add(generateTsFileResource(0));
    tsFileResourceList.add(generateTsFileResource(1));
    tsFileResourceList.add(generateTsFileResource(2));
  }

  @After
  public void tearDown() throws Exception {
    tsFileResourceList.clear();
  }

  @Test
  public void testAdd() {
    tsFileResourceList.add(generateTsFileResource(3));
    Assert.assertEquals(4, tsFileResourceList.size());
    Iterator<TsFileResource> iterator = tsFileResourceList.iterator();
    int index = 0;
    while (iterator.hasNext()) {
      Assert.assertSame(tsFileResources.get(index++), iterator.next());
    }
  }

  @Test
  public void testRemove() {
    tsFileResourceList.remove(tsFileResources.get(0));
    tsFileResources.remove(0);
    Assert.assertEquals(2, tsFileResourceList.size());
    Iterator<TsFileResource> iterator = tsFileResourceList.iterator();
    int index = 0;
    while (iterator.hasNext()) {
      Assert.assertSame(tsFileResources.get(index++), iterator.next());
    }
  }
}
