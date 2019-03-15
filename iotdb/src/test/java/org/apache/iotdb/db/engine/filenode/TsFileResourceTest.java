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
package org.apache.iotdb.db.engine.filenode;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TsFileResourceTest {


  private TsFileResource tsFileResource;

  public static TsFileResource constructTsfileResource() {
    TsFileResource tsFileResource;
    String relativePath = "relativePath";
    Map<String, Long> startTimes = new HashMap<>();
    Map<String, Long> endTimes = new HashMap<>();

    tsFileResource = new TsFileResource(OverflowChangeType.MERGING_CHANGE,
        relativePath);
    for (int i = 0; i < 10; i++) {
      startTimes.put("d" + i, (long) i);
    }
    for (int i = 0; i < 10; i++) {
      endTimes.put("d" + i, (long) (i + 10));
    }
    tsFileResource.setStartTimeMap(startTimes);
    tsFileResource.setEndTimeMap(endTimes);
    for (int i = 0; i < 5; i++) {
      tsFileResource.addMergeChanged("d" + i);
    }
    return tsFileResource;
  }

  @Before
  public void setUp() throws Exception {
    this.tsFileResource = constructTsfileResource();
  }

  @After
  public void tearDown() throws Exception {

  }

  @Test
  public void testSerDeialize() throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream(0);
    tsFileResource.serialize(outputStream);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    TsFileResource deTsfileResource = TsFileResource.deSerialize(inputStream);
    assertTsfileRecource(tsFileResource, deTsfileResource);
  }
  @Test
  public void testSerdeializeCornerCase() throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream(0);
    tsFileResource.setRelativePath(null);
    tsFileResource.serialize(outputStream);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    TsFileResource deTsfileResource = TsFileResource.deSerialize(inputStream);
    assertTsfileRecource(tsFileResource,deTsfileResource);
  }

  public static void assertTsfileRecource(TsFileResource tsFileResource,
      TsFileResource deTsfileResource) {
    assertEquals(tsFileResource.getBaseDirIndex(), deTsfileResource.getBaseDirIndex());
    assertEquals(tsFileResource.getRelativePath(), deTsfileResource.getRelativePath());
    assertEquals(tsFileResource.getOverflowChangeType(), deTsfileResource.getOverflowChangeType());
    assertEquals(tsFileResource.getStartTimeMap(), deTsfileResource.getStartTimeMap());
    assertEquals(tsFileResource.getEndTimeMap(), deTsfileResource.getEndTimeMap());
    assertEquals(tsFileResource.getMergeChanged(), deTsfileResource.getMergeChanged());
  }
}