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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class OSFileChannelTest {
  private static final File cacheDir = new File("target" + File.separator + "cache");
  private static final ObjectStorageConfig config =
      ObjectStorageDescriptor.getInstance().getConfig();
  private static final int cachePageSize = 100;
  @InjectMocks private OSFileCache cache = new OSFileCache(ObjectStorageType.TEST);

  @InjectMocks
  private OSFile testFile =
      new OSFile(new OSURI("test_bucket", "test_key"), ObjectStorageType.TEST);

  @Mock private ObjectStorageConnector connector;
  private int prevCachePageSize;
  private String[] prevCacheDirs;

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
      when(connector.getRemoteFile(testFile.toOSURI(), startPos, cachePageSize)).thenReturn(bytes);
      startPos += cachePageSize;
    }
  }

  @Test
  public void testReadFileSmallerThanPageSize() throws Exception {
    int size = 50;
    prepareOSFile(size);
    try (OSFileChannel channel = new OSFileChannel(testFile, cache)) {
      ByteBuffer dst = ByteBuffer.allocate(cachePageSize);
      assertEquals(size, channel.read(dst));
      dst.flip();
      for (int i = 0; i < size; ++i) {
        assertEquals((byte) i, dst.get());
      }
    }
  }

  @Test
  public void testReadFileBiggerThanPageSize() throws Exception {
    int size = 550;
    prepareOSFile(size);
    try (OSFileChannel channel = new OSFileChannel(testFile, cache)) {
      ByteBuffer dst = ByteBuffer.allocate(size);
      assertEquals(size, channel.read(dst));
      dst.flip();
      for (int i = 0; i < size; ++i) {
        assertEquals((byte) i, dst.get());
      }
    }
  }

  @Test
  public void testByteBufferBiggerThanFileSize() throws Exception {
    int fileSize = 550;
    int byteBufferSize = 600;
    prepareOSFile(fileSize);
    try (OSFileChannel channel = new OSFileChannel(testFile, cache)) {
      ByteBuffer dst = ByteBuffer.allocate(byteBufferSize);
      assertEquals(fileSize, channel.read(dst));
      assertEquals(byteBufferSize, dst.limit());
      dst.flip();
      for (int i = 0; i < fileSize; ++i) {
        assertEquals((byte) i, dst.get());
      }
    }
  }

  @Test
  public void testReadByPosition() throws Exception {
    int size = 550;
    prepareOSFile(size);
    try (OSFileChannel channel = new OSFileChannel(testFile, cache)) {
      int position = 0;
      while (position < size) {
        ByteBuffer dst = ByteBuffer.allocate(50);
        assertEquals(50, channel.read(dst, position));
        dst.flip();
        for (int i = 0; i < 50; ++i) {
          assertEquals((byte) (position + i), dst.get());
        }
        position += 50;
      }
    }
  }

  @Test
  public void testConcurrentRead() throws Exception {
    int size = 550;
    prepareOSFile(size);
    try (OSFileChannel channel = new OSFileChannel(testFile, cache)) {
      // random read by
      int threadsNum = 3;
      ExecutorService executorService = Executors.newFixedThreadPool(threadsNum);
      List<Future<Void>> futures = new ArrayList<>();
      for (int i = 0; i < threadsNum * 3; ++i) {
        Callable<Void> readTask =
            () -> {
              int position = 0;
              while (position < size) {
                ByteBuffer dst = ByteBuffer.allocate(50);
                assertEquals(50, channel.read(dst, position));
                dst.flip();
                for (int j = 0; j < 50; ++j) {
                  assertEquals((byte) (position + j), dst.get());
                }
                position += 50;
              }
              return null;
            };
        Future<Void> future = executorService.submit(readTask);
        futures.add(future);
      }
      // sequence read
      int position = 0;
      while (position < size) {
        ByteBuffer dst = ByteBuffer.allocate(50);
        assertEquals(50, channel.read(dst));
        dst.flip();
        for (int i = 0; i < 50; ++i) {
          assertEquals((byte) (position + i), dst.get());
        }
        position += 50;
      }
      // wait all random read tasks end
      for (Future<Void> future : futures) {
        future.get();
      }
    }
  }
}
