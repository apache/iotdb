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
package org.apache.iotdb.db.newsync.receiver.load;

import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.LoadFileException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.tools.TsFileRewriteTool;
import org.apache.iotdb.db.utils.FileLoaderUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/** This loader is used to load tsFiles. */
public class TsFileLoader implements ILoader {

  private String syncTask;
  private File tsFile;

  public TsFileLoader(String syncTask, File tsFile) {
    this.syncTask = syncTask;
    this.tsFile = tsFile;
  }

  @Override
  public boolean load() throws StorageEngineException, LoadFileException, MetadataException {
    TsFileResource tsFileResource = new TsFileResource(tsFile);
    tsFileResource.setClosed(true);
    try {
      FileLoaderUtils.checkTsFileResource(tsFileResource);
      List<TsFileResource> splitResources = new ArrayList();
      if (tsFileResource.isSpanMultiTimePartitions()) {
        TsFileRewriteTool.rewriteTsFile(tsFileResource, splitResources);
        tsFileResource.writeLock();
        tsFileResource.removeModFile();
        tsFileResource.writeUnlock();
      }

      if (splitResources.isEmpty()) {
        splitResources.add(tsFileResource);
      }

      for (TsFileResource resource : splitResources) {
        StorageEngine.getInstance().loadNewTsFile(resource);
      }
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
