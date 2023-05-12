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

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;

public class OSFileCacheImpl implements OSFileCache {

  private OSFileCacheImpl() {

  }

  @Override
  public InputStream getAsInputSteam(String fileName, long startPosition) throws IOException {
    FileChannel cacheFileChannel = getLocalCacheFileChannel(fileName, startPosition);
    return Channels.newInputStream(cacheFileChannel);
  }

  private FileChannel getLocalCacheFileChannel(String fileName, long startPosition) {
    // 根据 fileName 和 startPosition 计算出对应的本地文件路径，并返回对应的 FileChannel
    // 如果是使用一个 CacheFile, 则寻找到对应的位置，可能需要封装一个自己的 FileChannel 防止读多
    return null;
  }

  public static OSFileCacheImpl getInstance() {
    return OSFileCacheImpl.InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {
    private InstanceHolder() {}

    private static final OSFileCacheImpl INSTANCE = new OSFileCacheImpl();
  }
}
