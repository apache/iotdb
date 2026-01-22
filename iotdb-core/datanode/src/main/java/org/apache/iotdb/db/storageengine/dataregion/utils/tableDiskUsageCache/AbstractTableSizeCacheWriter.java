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

package org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageCache;

import org.apache.iotdb.db.storageengine.StorageEngine;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class AbstractTableSizeCacheWriter {
  protected static final String TEMP_CACHE_FILE_SUBFIX = ".tmp";
  protected final int regionId;
  protected long previousCompactionTimestamp = System.currentTimeMillis();
  protected long lastWriteTimestamp = System.currentTimeMillis();
  protected int currentIndexFileVersion = 0;
  protected final File dir;

  public AbstractTableSizeCacheWriter(String database, int regionId) {
    this.regionId = regionId;
    this.dir = StorageEngine.getDataRegionSystemDir(database, regionId + "");
  }

  protected void failedToRecover(Exception e) {
    TableDiskUsageCache.getInstance().failedToRecover(e);
  }

  protected void deleteOldVersionFiles(int maxVersion, String prefix, List<File> files) {
    for (File file : files) {
      try {
        int version = Integer.parseInt(file.getName().substring(prefix.length()));
        if (version != maxVersion) {
          Files.deleteIfExists(file.toPath());
        }
      } catch (Exception e) {
      }
    }
  }

  public void closeIfIdle() {
    if (System.currentTimeMillis() - lastWriteTimestamp >= TimeUnit.MINUTES.toMillis(1)) {
      close();
    }
  }

  public abstract boolean needCompact();

  public abstract void compact();

  public abstract void flush() throws IOException;

  public abstract void sync() throws IOException;

  public abstract void close();
}
