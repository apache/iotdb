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
package org.apache.iotdb.db.utils;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.VersionUtils;
import org.junit.Assert;
import org.junit.Test;

public class VersionUtilsTest {

  @Test
  public void uncompleteFileTest() {
    List<ChunkMetadata> chunkMetadataList = new ArrayList<>();
    chunkMetadataList.add(new ChunkMetadata("s1", TSDataType.INT32, 10, null));
    chunkMetadataList.add(new ChunkMetadata("s1", TSDataType.INT32, 20, null));
    chunkMetadataList.add(new ChunkMetadata("s1", TSDataType.INT32, 30, null));
    chunkMetadataList.add(new ChunkMetadata("s1", TSDataType.INT32, 40, null));
    chunkMetadataList.add(new ChunkMetadata("s1", TSDataType.INT32, 50, null));

    List<Pair<Long, Long>> versionInfo = new ArrayList<>();
    versionInfo.add(new Pair<>(25L, 1L));
    versionInfo.add(new Pair<>(45L, 2L));

    VersionUtils.applyVersion(chunkMetadataList, versionInfo);

    Assert.assertEquals(1L, chunkMetadataList.get(0).getVersion());
    Assert.assertEquals(1L, chunkMetadataList.get(1).getVersion());
    Assert.assertEquals(2L, chunkMetadataList.get(2).getVersion());
    Assert.assertEquals(2L, chunkMetadataList.get(3).getVersion());
    Assert.assertEquals(0L, chunkMetadataList.get(4).getVersion());
  }


}
