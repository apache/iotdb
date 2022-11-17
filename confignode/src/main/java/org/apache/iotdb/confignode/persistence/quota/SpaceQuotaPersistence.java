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

package org.apache.iotdb.confignode.persistence.quota;

import org.apache.iotdb.common.rpc.thrift.TSpaceQuota;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.commons.utils.IOUtils;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class SpaceQuotaPersistence {

  private static final Logger logger = LoggerFactory.getLogger(SpaceQuotaPersistence.class);
  private static final String SUFFIX = ".quota";
  private static final String TEMP_SUFFIX = ".temp";
  private String spaceQuotaDirPath;

  /** SingleTone */
  private static class LocalFileSpaceQuotaManagerHolder {
    private static final SpaceQuotaPersistence INSTANCE = new SpaceQuotaPersistence();

    private LocalFileSpaceQuotaManagerHolder() {}
  }

  public static SpaceQuotaPersistence getInstance() {
    return SpaceQuotaPersistence.LocalFileSpaceQuotaManagerHolder.INSTANCE;
  }

  public SpaceQuotaPersistence() {
    this.spaceQuotaDirPath = ConfigNodeDescriptor.getInstance().getConf().getSpaceQuotaDir();
  }

  public void saveSpaceQuota(String path, TSpaceQuota spaceQuota) throws IOException {
    File quotaProfile =
        SystemFileFactory.INSTANCE.getFile(
            spaceQuotaDirPath + File.separator + path + SUFFIX + TEMP_SUFFIX);
    try (OutputStream out = new FileOutputStream(quotaProfile);
        DataOutputStream outputStream = new DataOutputStream(out)) {
      BasicStructureSerDeUtil.write(path, outputStream);
      BasicStructureSerDeUtil.write(spaceQuota.getDeviceNum(), outputStream);
      BasicStructureSerDeUtil.write(spaceQuota.getTimeserieNum(), outputStream);
      BasicStructureSerDeUtil.write(spaceQuota.getDiskSize(), outputStream);
      outputStream.flush();
    } catch (FileNotFoundException e) {
      logger.error("file {} not found.", quotaProfile, e);
    }
    File oldFile =
        SystemFileFactory.INSTANCE.getFile(spaceQuotaDirPath + File.separator + path + SUFFIX);
    IOUtils.replaceFile(quotaProfile, oldFile);
  }

  public Map<String, TSpaceQuota> loadSpaceQuota(String path) throws IOException {
    File quotaProfile =
        SystemFileFactory.INSTANCE.getFile(spaceQuotaDirPath + File.separator + path);
    Map<String, TSpaceQuota> spaceQuotaLimit = new HashMap<>();
    if (!quotaProfile.exists() || !quotaProfile.isFile()) {
      // System may crush before a newer file is renamed.
      File newProfile =
          SystemFileFactory.INSTANCE.getFile(
              spaceQuotaDirPath
                  + File.separator
                  + path
                  + IoTDBConstant.PROFILE_SUFFIX
                  + TEMP_SUFFIX);
      if (newProfile.exists() && newProfile.isFile()) {
        if (!newProfile.renameTo(quotaProfile)) {
          logger.error("New profile renaming not succeed.");
        }
        quotaProfile = newProfile;
      } else {
        logger.error("file {} not found.", quotaProfile);
      }
    }

    try (DataInputStream inputStream =
        new DataInputStream(new BufferedInputStream(new FileInputStream(quotaProfile)))) {
      byte[] buffer = new byte[1024];
      inputStream.read(buffer);
      ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
      String prefixPath = BasicStructureSerDeUtil.readString(byteBuffer);
      int deviceNum = BasicStructureSerDeUtil.readInt(byteBuffer);
      int timeserieNum = BasicStructureSerDeUtil.readInt(byteBuffer);
      long disk = BasicStructureSerDeUtil.readLong(byteBuffer);
      TSpaceQuota spaceLimit = new TSpaceQuota();
      spaceLimit.setDeviceNum(deviceNum);
      spaceLimit.setTimeserieNum(timeserieNum);
      spaceLimit.setDiskSize(disk);
      spaceQuotaLimit.put(prefixPath, spaceLimit);
    }
    return spaceQuotaLimit;
  }

  public void init(Map<String, TSpaceQuota> spaceQuotaLimit) {
    File file = new File(spaceQuotaDirPath);
    String[] paths = file.list();
    if (paths.length != 0) {
      for (String path : paths) {
        try {
          spaceQuotaLimit.putAll(loadSpaceQuota(path));
        } catch (IOException e) {
          logger.error("An error occurred while recovering data. Storage group{}", path, e);
        }
      }
    }
  }
}
