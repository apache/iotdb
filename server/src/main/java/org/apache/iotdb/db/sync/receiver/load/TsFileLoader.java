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
package org.apache.iotdb.db.sync.receiver.load;

import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;
import org.apache.iotdb.db.exception.sync.PipeDataLoadException;
import org.apache.iotdb.db.exception.sync.PipeDataLoadUnbearableException;
import org.apache.iotdb.db.tools.TsFileRewriteTool;
import org.apache.iotdb.db.utils.FileLoaderUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/** This loader is used to load tsFiles. If .mods file exists, it will be loaded as well. */
public class TsFileLoader implements ILoader {

  private File tsFile;

  public TsFileLoader(File tsFile) {
    this.tsFile = tsFile;
  }

  @Override
  public void load() throws PipeDataLoadException {
    try {
      TsFileResource tsFileResource = new TsFileResource(tsFile);
      tsFileResource.setStatus(TsFileResourceStatus.CLOSED);
      FileLoaderUtils.loadOrGenerateResource(tsFileResource);
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
        StorageEngine.getInstance().loadNewTsFile(resource, false);
      }
    } catch (Exception e) {
      throw new PipeDataLoadUnbearableException(e.getMessage());
    }
  }
}
