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

package com.timecho.iotdb.commons.commission;

import org.apache.iotdb.commons.conf.Edition;
import org.apache.iotdb.commons.conf.ModuleConfigManager;
import org.apache.iotdb.commons.exception.LicenseException;
import org.apache.iotdb.commons.i18n.CommissionMessages;

import java.util.Properties;

/** Validates that a license was generated for the product release running this process. */
public final class LicenseReleaseValidator {

  private LicenseReleaseValidator() {}

  /**
   * Validates the release stored in a decrypted license payload.
   *
   * <p>Legacy licenses do not have a release field. They are not bound to either product release
   * for backward compatibility and can activate both TimechoDB and IoTDB builds. Licenses that do
   * carry a release field must still match the current product release.
   */
  public static void validateForCurrentRelease(Properties licenseProperties)
      throws LicenseException {
    validate(licenseProperties, ModuleConfigManager.getInstance().getEdition());
  }

  static void validate(Properties licenseProperties, Edition currentEdition)
      throws LicenseException {
    String rawRelease = licenseProperties.getProperty(Lottery.PRODUCT_RELEASE_NAME);
    // Older license formats do not carry the R1 field. An empty field is also treated as
    // unreadable so those licenses remain compatible with both product releases.
    if (rawRelease == null || rawRelease.trim().isEmpty()) {
      return;
    }
    int licenseRelease = resolve(rawRelease);
    if (licenseRelease != currentEdition.getRelease()) {
      throw new LicenseException(
          String.format(
              CommissionMessages
                  .EXCEPTION_LICENSE_RELEASE_ARG_DOES_NOT_MATCH_CURRENT_RUNNING_RELEASE_ARG_2FDF322A,
              getReleaseDisplayName(licenseRelease),
              currentEdition.getDisplayName()));
    }
  }

  private static int resolve(String rawRelease) throws LicenseException {
    try {
      int release = Integer.parseInt(rawRelease.trim());
      if (release == Edition.TIMECHODB.getRelease() || release == Edition.IOTDB.getRelease()) {
        return release;
      }
    } catch (NumberFormatException ignored) {
      // The common invalid-value path below provides the localized error message.
    }
    throw new LicenseException(
        String.format(
            CommissionMessages.EXCEPTION_LICENSE_RELEASE_ARG_IS_INVALID_77533A6B, rawRelease));
  }

  private static String getReleaseDisplayName(int release) {
    return release == Edition.TIMECHODB.getRelease()
        ? Edition.TIMECHODB.getDisplayName()
        : Edition.IOTDB.getDisplayName();
  }
}
