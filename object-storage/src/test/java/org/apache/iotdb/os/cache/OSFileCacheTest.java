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
package org.apache.iotdb.os.cache;

import org.apache.iotdb.os.conf.ObjectStorageConfig;
import org.apache.iotdb.os.conf.ObjectStorageDescriptor;
import org.apache.iotdb.os.fileSystem.OSFile;
import org.apache.iotdb.os.fileSystem.OSURI;
import org.apache.iotdb.os.io.ObjectStorageConnector;
import org.apache.iotdb.os.utils.ObjectStorageType;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class OSFileCacheTest {
  private static final File cacheDir = new File("target" + File.separator + "cache");
  private static final ObjectStorageConfig config =
      ObjectStorageDescriptor.getInstance().getConfig();
  @InjectMocks private OSFileCache cache = new OSFileCache(ObjectStorageType.TEST);

  @InjectMocks
  private OSFile testFile =
      new OSFile(new OSURI("test_bucket", "test_key"), ObjectStorageType.TEST);

  @Mock private ObjectStorageConnector connector;
  private static ObjectStorageType prevObjectStorageType;
  private int prevCachePageSize;
  private String[] prevCacheDirs;

  @BeforeClass
  public static void beforeClass() throws Exception {
    prevObjectStorageType = config.getOsType();
    config.setOsType(ObjectStorageType.TEST);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    config.setOsType(prevObjectStorageType);
  }

  @Before
  public void setUp() throws Exception {
    prevCachePageSize = config.getCachePageSize();
    config.setCachePageSize(100);
    prevCacheDirs = config.getCacheDirs();
    config.setCacheDirs(new String[] {cacheDir.getPath()});
    cacheDir.mkdirs();
    CacheFileManager.getInstance().setCacheFileId(0);
  }

  @After
  public void tearDown() throws Exception {
    config.setCachePageSize(prevCachePageSize);
    config.setCacheDirs(prevCacheDirs);
    FileUtils.deleteDirectory(cacheDir);
  }

  @Test
  public void testGet() throws Exception {
    // pull 0-100, and 0-100 exists
    byte[] bytes0to100 = new byte[100];
    for (int i = 0; i < 100; ++i) {
      bytes0to100[i] = (byte) i;
    }
    when(connector.getRemoteObject(testFile.toOSURI(), 0, 100)).thenReturn(bytes0to100);
    OSFileCacheKey key0to100 = new OSFileCacheKey(testFile, 0);
    OSFileCacheValue value0to100 = cache.get(key0to100);
    assertTrue(value0to100.getCacheFile().exists());
    assertEquals(value0to100.getCacheFile().length(), value0to100.getLength());
    try (FileChannel channel =
        FileChannel.open(value0to100.getCacheFile().toPath(), StandardOpenOption.READ)) {
      byte[] readBytes = new byte[100];
      ByteBuffer buffer = ByteBuffer.wrap(readBytes);
      int bytesNum =
          channel.read(
              buffer, value0to100.getStartPositionInCacheFile() + value0to100.getMetaSize());
      assertEquals(100, bytesNum);
      assertEquals(100, value0to100.getDataSize());
      assertArrayEquals(bytes0to100, readBytes);
    }
    // pull 100-200, but only 100-150 exists
    byte[] bytes100to150 = new byte[150 - 100];
    for (int i = 0; i < 150 - 100; ++i) {
      bytes100to150[i] = (byte) i;
    }
    when(connector.getRemoteObject(testFile.toOSURI(), 100, 100)).thenReturn(bytes100to150);
    OSFileCacheKey key100to150 = new OSFileCacheKey(testFile, 100);
    OSFileCacheValue value100to150 = cache.get(key100to150);
    assertTrue(value100to150.getCacheFile().exists());
    try (FileChannel channel =
        FileChannel.open(value100to150.getCacheFile().toPath(), StandardOpenOption.READ)) {
      byte[] readBytes = new byte[50];
      ByteBuffer buffer = ByteBuffer.wrap(readBytes);
      int bytesNum =
          channel.read(
              buffer, value100to150.getStartPositionInCacheFile() + value100to150.getMetaSize());
      assertEquals(50, bytesNum);
      assertEquals(50, value100to150.getDataSize());
      assertArrayEquals(bytes100to150, readBytes);
    }
  }
}
