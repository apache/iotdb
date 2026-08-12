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
import org.apache.iotdb.commons.exception.LicenseException;

import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class LicenseReleaseValidatorTest {

  @Test
  public void testMatchingRelease() throws Exception {
    Properties timechoDBLicense = licenseFor(Edition.TIMECHODB.getRelease());
    Properties ioTDBLicense = licenseFor(Edition.IOTDB.getRelease());

    LicenseReleaseValidator.validate(timechoDBLicense, Edition.TIMECHODB);
    LicenseReleaseValidator.validate(ioTDBLicense, Edition.IOTDB);
  }

  @Test
  public void testMismatchedRelease() {
    LicenseException exception =
        Assert.assertThrows(
            LicenseException.class,
            () ->
                LicenseReleaseValidator.validate(
                    licenseFor(Edition.IOTDB.getRelease()), Edition.TIMECHODB));

    Assert.assertTrue(exception.getMessage().contains("IoTDB"));
    Assert.assertTrue(exception.getMessage().contains("TimechoDB"));
    Assert.assertTrue(exception.getMessage().contains("current running release"));
  }

  @Test
  public void testLegacyLicenseIsAcceptedByBothReleases() throws Exception {
    Properties legacyLicense = new Properties();

    LicenseReleaseValidator.validate(legacyLicense, Edition.TIMECHODB);
    LicenseReleaseValidator.validate(legacyLicense, Edition.IOTDB);
  }

  @Test
  public void testBlankReleaseIsAcceptedByBothReleases() throws Exception {
    Properties licenseWithBlankRelease = new Properties();
    licenseWithBlankRelease.setProperty(Lottery.PRODUCT_RELEASE_NAME, " ");
    Properties licenseWithEmptyRelease = new Properties();
    licenseWithEmptyRelease.setProperty(Lottery.PRODUCT_RELEASE_NAME, "");

    LicenseReleaseValidator.validate(licenseWithBlankRelease, Edition.TIMECHODB);
    LicenseReleaseValidator.validate(licenseWithBlankRelease, Edition.IOTDB);
    LicenseReleaseValidator.validate(licenseWithEmptyRelease, Edition.TIMECHODB);
    LicenseReleaseValidator.validate(licenseWithEmptyRelease, Edition.IOTDB);
  }

  @Test
  public void testInvalidReleaseRejected() {
    Properties license = new Properties();
    license.setProperty(Lottery.PRODUCT_RELEASE_NAME, "99");

    Assert.assertThrows(
        LicenseException.class, () -> LicenseReleaseValidator.validate(license, Edition.TIMECHODB));
  }

  private static Properties licenseFor(int release) {
    Properties license = new Properties();
    license.setProperty(Lottery.PRODUCT_RELEASE_NAME, String.valueOf(release));
    return license;
  }
}
