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
package org.apache.iotdb.db.engine.upgrade;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.tool.upgrade.UpgradeTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpgradeTask implements Runnable {

  private final TsFileResource upgradeResource;
  private static final Logger logger = LoggerFactory.getLogger(UpgradeTask.class);

  public UpgradeTask(TsFileResource upgradeResource) {
    this.upgradeResource = upgradeResource;
  }

  @Override
  public void run() {
    try {
      upgradeResource.getWriteQueryLock().readLock().lock();
      String tsfilePathBefore = upgradeResource.getFile().getAbsolutePath();
      String tsfilePathAfter =
          upgradeResource.getFile().getParentFile().getParent() + File.separator + "tmp"
              + File.separator + "tmp_" + upgradeResource
              .getFile().getName();
      try {
        UpgradeTool.upgradeOneTsfile(tsfilePathBefore, tsfilePathAfter);
      } catch (IOException e) {
        logger.error("generate upgrade file failed, the file to be upgraded:{}", tsfilePathBefore);
      } finally {
        upgradeResource.getWriteQueryLock().readLock().unlock();
      }
      upgradeResource.getWriteQueryLock().writeLock().lock();
      try {
        String tmpTsfilePath =
            upgradeResource.getFile().getParentFile().getAbsolutePath() + File.separator + "tmp_"
                + upgradeResource.getFile().getName();
        FileUtils.copyFile(new File(tsfilePathAfter), new File(tmpTsfilePath));
        new File(tsfilePathBefore).delete();
        new File(tsfilePathAfter).delete();
        new File(tsfilePathAfter).getParentFile().delete();
        new File(tmpTsfilePath).renameTo(new File(tsfilePathBefore));
      } finally {
        upgradeResource.getWriteQueryLock().writeLock().unlock();
      }
      logger.info("Upgrade completes, file path:{}", tsfilePathBefore);
    } catch (Exception e) {
      logger.error("meet error when upgrade file:{} :", upgradeResource.getFile().getAbsolutePath(),
          e);
    }
  }
}
