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

package org.apache.iotdb.confignode.manager.regulate;

import org.apache.iotdb.commons.exception.LicenseException;

import com.timecho.iotdb.manager.TimechoConfigManager;
import com.timecho.iotdb.manager.regulate.RegulateManager;

import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;

/** License Manager without system info check, only for IoTDBActivationTest */
public class RegulateManagerWithoutEncryptionAndMachineBinding extends RegulateManager {

  public RegulateManagerWithoutEncryptionAndMachineBinding(TimechoConfigManager configManager)
      throws LicenseException {
    super(configManager);
  }

  @Override
  public boolean verifyAllSystemInfo(Properties properties) {
    return true;
  }

  @Override
  protected Properties loadLicenseFromEveryVersion(String licenseContent)
      throws LicenseException, IOException {
    String licenseVersion = RegulateManager.checkLicenseVersion(licenseContent);
    if ("02".equals(licenseVersion) || "03".equals(licenseVersion)) {
      return super.loadLicenseFromEveryVersion(licenseContent);
    }
    Properties licenseProperties = new Properties();
    licenseProperties.load(new StringReader(licenseContent));
    return licenseProperties;
  }
}
