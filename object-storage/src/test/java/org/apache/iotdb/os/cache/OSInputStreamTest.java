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
import org.apache.iotdb.os.exception.ObjectStorageException;
import org.apache.iotdb.os.fileSystem.OSFile;
import org.apache.iotdb.os.fileSystem.OSURI;
import org.apache.iotdb.os.io.ObjectStorageConnector;
import org.apache.iotdb.os.io.aws.S3MetaData;
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
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class OSInputStreamTest {
  private static final File cacheDir = new File("target" + File.separator + "cache");
  private static final ObjectStorageConfig config =
      ObjectStorageDescriptor.getInstance().getConfig();
  private static final int cachePageSize = 100;
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
    config.setCachePageSize(cachePageSize);
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

  private void prepareOSFile(int size) throws ObjectStorageException {
    when(connector.getMetaData(testFile.toOSURI()))
        .thenReturn(new S3MetaData(size, System.currentTimeMillis()));
    int startPos = 0;
    while (startPos < size) {
      byte[] bytes = new byte[(int) Math.min(cachePageSize, size - startPos)];
      for (int i = 0; i < bytes.length; ++i) {
        bytes[i] = (byte) (startPos + i);
      }
      when(connector.getRemoteObject(testFile.toOSURI(), startPos, cachePageSize))
          .thenReturn(bytes);
      startPos += cachePageSize;
    }
  }

  @Test
  public void testReadByte() throws Exception {
    int size = 50;
    prepareOSFile(size);
    try (OSFileChannel channel = new OSFileChannel(testFile, cache);
        InputStream in = OSFileChannel.newInputStream(channel)) {
      for (int i = 0; in.available() > 0; ++i) {
        assertEquals((byte) i, in.read());
      }
      assertEquals(0, in.available());
    }
  }

  @Test
  public void testReadByteArray() throws Exception {
    int size = 128;
    prepareOSFile(size);
    try (OSFileChannel channel = new OSFileChannel(testFile, cache);
        InputStream in = OSFileChannel.newInputStream(channel)) {
      int position = 0;
      while (position < size) {
        byte[] bytes = new byte[50];
        assertEquals(Math.min(50, size - position), in.read(bytes));
        for (int i = 0; i < Math.min(50, size - position); ++i) {
          assertEquals((byte) (position + i), bytes[i]);
        }
        position += 50;
      }
      assertEquals(0, in.available());
    }
  }

  @Test
  public void testReadByteArrayByLen() throws Exception {
    int size = 550;
    prepareOSFile(size);
    try (OSFileChannel channel = new OSFileChannel(testFile, cache);
        InputStream in = OSFileChannel.newInputStream(channel)) {
      byte[] bytes = new byte[size];
      int position = 0;
      while (position < size) {
        assertEquals(50, in.read(bytes, position, 50));
        for (int i = 0; i < 50; ++i) {
          assertEquals((byte) (position + i), bytes[position + i]);
        }
        position += 50;
      }
      assertEquals(0, in.available());
    }
  }

  @Test
  public void testSkip() throws Exception {
    int size = 550;
    prepareOSFile(size);
    try (OSFileChannel channel = new OSFileChannel(testFile, cache);
        InputStream in = OSFileChannel.newInputStream(channel)) {
      byte[] bytes = new byte[size];
      int position = 0;
      while (position < size) {
        if (position % 100 == 0) {
          in.skip(50);
          position += 50;
          continue;
        }
        assertEquals(50, in.read(bytes, position, 50));
        for (int i = 0; i < 50; ++i) {
          assertEquals((byte) (position + i), bytes[position + i]);
        }
        position += 50;
      }
      assertEquals(0, in.available());
    }
  }
}
