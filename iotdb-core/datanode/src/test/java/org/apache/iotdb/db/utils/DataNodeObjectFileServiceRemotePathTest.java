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

package org.apache.iotdb.db.utils;

import com.timecho.iotdb.os.utils.RemoteStorageBlock;
import org.apache.tsfile.fileSystem.FSType;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class DataNodeObjectFileServiceRemotePathTest {

  @Test
  public void objectRootFromOsTsFilePath() {
    Optional<String> root =
        DataNodeObjectFileService.objectRootFromTsFileRemotePath(
            "os://bucket/3/sequence/root.db/1/0/1-1-0-0.tsfile");
    assertEquals(Optional.of("os://bucket/3/object"), root);
  }

  @Test
  public void objectRootFromUnseqTsFilePath() {
    Optional<String> root =
        DataNodeObjectFileService.objectRootFromTsFileRemotePath(
            "os://bucket/7/unsequence/root.db/1/0/1-1-0-0.tsfile");
    assertEquals(Optional.of("os://bucket/7/object"), root);
  }

  @Test
  public void objectRootFromRemoteStorageBlock() {
    RemoteStorageBlock block =
        new RemoteStorageBlock(
            FSType.OBJECT_STORAGE, "os://iotdb-minio/2/sequence/root.sg/9/0/a.tsfile");
    assertEquals(
        Optional.of("os://iotdb-minio/2/object"),
        DataNodeObjectFileService.objectRootFromRemoteStorageBlock(block));
  }

  @Test
  public void objectRootMissingWhenPathHasNoSeqFolder() {
    assertFalse(
        DataNodeObjectFileService.objectRootFromTsFileRemotePath("os://bucket/3/object/1/a.bin")
            .isPresent());
    assertFalse(DataNodeObjectFileService.objectRootFromRemoteStorageBlock(null).isPresent());
  }
}
