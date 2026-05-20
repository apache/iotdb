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

import org.apache.iotdb.common.rpc.thrift.TLicense;
import org.apache.iotdb.commons.exception.LicenseException;

import com.timecho.iotdb.commons.commission.obligation.Obligation;
import com.timecho.iotdb.commons.commission.obligation.ObligationAllowAbsent;
import com.timecho.iotdb.commons.commission.obligation.ObligationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Lottery {
  private static final Logger logger = LoggerFactory.getLogger(Lottery.class);

  // license common fields
  public static final String LICENSE_ISSUE_TIMESTAMP_NAME = "L1";
  public static final String LICENSE_EXPIRE_TIMESTAMP_NAME = "L2";
  public static final String DISCONNECTION_FROM_ACTIVE_NODE_TIME_LIMIT_NAME = "L3";
  public static final String SKIP_HARDWARE_SYSTEM_INFO_CHECK_NAME = "L4";

  // system info fields
  public static final String CPU_ID_NAME = "S1";
  public static final String MAIN_BOARD_ID_NAME = "S2";
  public static final String SYSTEM_UUID_NAME = "S3";
  public static final String IP_ADDRESS_NAME = "S4";
  public static final String INTERNAL_PORT_NAME = "S5";
  public static final String IS_SEED_CONFIGNODE_NODE_NAME = "S6";
  public static final String CLUSTER_NAME_NAME = "S7";
  public static final String NODE_UUID_NAME = "S8";
  public static final String SYSTEM_INFO_HASH = "S9";

  // DataNode fields
  public static final String DATANODE_NUM_LIMIT_NAME = "DN1";
  public static final String DATANODE_CPU_CORE_NUM_LIMIT_NAME = "DN2";
  public static final String DEVICE_NUM_LIMIT_NAME = "DN3";
  public static final String SENSOR_NUM_LIMIT_NAME = "DN4";

  // AINode fields
  public static final String AINODE_NUM_LIMIT_NAME = "ML1";

  // Release fields
  public static final String RELEASE_TYPE_NAME = "TYP";
  public static final String RELEASE_TYPE_TRIAL = "trial";

  // activate info
  protected final Obligation<Long> licenseIssueTimestamp = new Obligation<>(0L, Long::parseLong);
  protected final Obligation<Long> licenseExpireTimestamp = new Obligation<>(0L, Long::parseLong);
  protected final Obligation<Long> disconnectionFromActiveNodeTimeObligation =
      new ObligationAllowAbsent<>(0L, Long::parseLong, TimeUnit.DAYS.toMillis(7));
  protected final Obligation<Boolean> skipHardwareSystemInfoCheck =
      new Obligation<>(false, Boolean::parseBoolean);
  protected final Obligation<Short> dataNodeNumObligation =
      new ObligationAllowAbsent<>((short) 0, Short::parseShort, Short.MAX_VALUE);
  protected final Obligation<Integer> dataNodeCpuCoreNumObligation =
      new ObligationAllowAbsent<>(0, Integer::parseInt, Integer.MAX_VALUE);
  protected final Obligation<Long> deviceNumObligation =
      new ObligationAllowAbsent<>(0L, Long::parseLong, Long.MAX_VALUE);
  protected final Obligation<Long> sensorNumObligation =
      new ObligationAllowAbsent<>(0L, Long::parseLong, Long.MAX_VALUE);
  protected final Obligation<Short> aiNodeNumObligation =
      new ObligationAllowAbsent<>((short) 0, Short::parseShort, (short) 0);
  private final List<Obligation<?>> allObligation;

  // other info
  protected enum LicenseSource {
    FROM_FILE,
    FROM_REMOTE,
    UNKNOWN,
    NO_LICENSE
  }

  protected LicenseSource licenseSource = LicenseSource.UNKNOWN;

  protected final Runnable onLicenseChange;
  private static final Runnable DO_NOTHING_WHEN_LICENSE_CHANGE = () -> {};

  private ObligationStatus oldActivateStatus = ObligationStatus.UNKNOWN;

  public static final List<Integer> CAPACITY_CONFIG = new ArrayList<>();

  // endregion

  static {
    CAPACITY_CONFIG.add(1);
    CAPACITY_CONFIG.add(4);
    CAPACITY_CONFIG.add(8);
    CAPACITY_CONFIG.add(1);
  }

  public Lottery(Runnable onLicenseChange) {
    this.onLicenseChange = onLicenseChange;
    allObligation =
        Arrays.asList(
            licenseIssueTimestamp,
            licenseExpireTimestamp,
            disconnectionFromActiveNodeTimeObligation,
            skipHardwareSystemInfoCheck,
            dataNodeNumObligation,
            dataNodeCpuCoreNumObligation,
            deviceNumObligation,
            sensorNumObligation,
            aiNodeNumObligation);
  }

  public Lottery() {
    this(DO_NOTHING_WHEN_LICENSE_CHANGE);
  }

  // region getter and setter

  public long getLicenseExpireTimestamp() {
    return licenseExpireTimestamp.getValue();
  }

  public long getLicenseIssueTimestamp() {
    return licenseIssueTimestamp.getValue();
  }

  public short getDataNodeNumLimit() {
    return dataNodeNumObligation.getValue();
  }

  public int getDataNodeCpuCoreNumLimit() {
    return dataNodeCpuCoreNumObligation.getValue();
  }

  public long getDeviceNumLimit() {
    return deviceNumObligation.getValue();
  }

  public long getSensorNumLimit() {
    return sensorNumObligation.getValue();
  }

  public long getDisconnectionFromActiveNodeTimeLimit() {
    return this.disconnectionFromActiveNodeTimeObligation.getValue();
  }

  public short getAINodeNumLimit() {
    return this.aiNodeNumObligation.getValue();
  }

  // endregion

  public boolean reset() {
    if (licenseSource.equals(LicenseSource.NO_LICENSE)) {
      return false;
    }
    for (Obligation<?> obligation : allObligation) {
      obligation.reset();
    }
    licenseSource = LicenseSource.NO_LICENSE;
    this.onLicenseChange.run();
    logActivateStatus(true);
    return true;
  }

  // region load method

  public boolean loadFromProperties(Properties properties, boolean needLog)
      throws LicenseException {
    // try load properties
    Lottery newLottery = new Lottery(null);
    // activate info
    try {
      // To add a new license field, set a default value for compatible with older license version
      newLottery.licenseIssueTimestamp.parse(
          properties.getProperty(
              LICENSE_ISSUE_TIMESTAMP_NAME, String.valueOf(System.currentTimeMillis())));
      newLottery.licenseExpireTimestamp.parse(
          properties.getProperty(LICENSE_EXPIRE_TIMESTAMP_NAME));
      newLottery.skipHardwareSystemInfoCheck.parse(
          properties.getProperty(SKIP_HARDWARE_SYSTEM_INFO_CHECK_NAME));
      newLottery.dataNodeNumObligation.parse(properties.getProperty(DATANODE_NUM_LIMIT_NAME));
      newLottery.dataNodeCpuCoreNumObligation.parse(
          properties.getProperty(DATANODE_CPU_CORE_NUM_LIMIT_NAME));
      newLottery.deviceNumObligation.parse(properties.getProperty(DEVICE_NUM_LIMIT_NAME));
      newLottery.sensorNumObligation.parse(properties.getProperty(SENSOR_NUM_LIMIT_NAME));
      newLottery.disconnectionFromActiveNodeTimeObligation.parse(
          properties.getProperty(DISCONNECTION_FROM_ACTIVE_NODE_TIME_LIMIT_NAME));
      newLottery.aiNodeNumObligation.parse(properties.getProperty(AINODE_NUM_LIMIT_NAME, "0"));
    } catch (Exception e) {
      logger.error("License parse error", e);
      return false;
    }

    // compare and copy
    if (needLog) {
      this.logLicenseDifferences(newLottery);
    }
    this.copyFrom(newLottery);
    this.licenseSource = LicenseSource.FROM_FILE;

    // if activate status change, log
    if (needLog) {
      logActivateStatus(false);
    }

    this.onLicenseChange.run();

    return true;
  }

  public void loadFromTLicense(TLicense license) throws LicenseException {
    Lottery newLottery = new Lottery(null);
    newLottery.licenseIssueTimestamp.setValue(license.licenseIssueTimestamp);
    newLottery.licenseExpireTimestamp.setValue(license.getExpireTimestamp());
    newLottery.disconnectionFromActiveNodeTimeObligation.setValue(
        license.getDisconnectionFromActiveNodeTime());
    newLottery.dataNodeCpuCoreNumObligation.setValue(license.cpuCoreNum);
    newLottery.dataNodeNumObligation.setValue(license.dataNodeNum);
    newLottery.deviceNumObligation.setValue(license.deviceNum);
    newLottery.sensorNumObligation.setValue(license.sensorNum);
    newLottery.aiNodeNumObligation.setValue(license.getAiNodeNum());

    // compare and copy
    this.logLicenseDifferences(newLottery);
    this.copyFrom(newLottery);
    this.licenseSource = LicenseSource.FROM_REMOTE;

    // if activate status change, log
    logActivateStatus(false);

    this.onLicenseChange.run();
  }

  // endregion

  // region helper method

  private long getLong(String key, Properties properties) throws LicenseException {
    if (!properties.containsKey(key)) {
      throw new LicenseException(
          String.format("%s is necessary, but cannot be found in license file", key));
    }
    try {
      return Long.parseLong(properties.getProperty(key).trim());
    } catch (NumberFormatException e) {
      throw new LicenseException(key + " field cannot be parsed to Long", e);
    }
  }

  public void logActivateStatus(boolean onlyForStatusChange) {
    ObligationStatus nowActivateStatus = getActivateStatus();
    if (onlyForStatusChange) {
      if (!nowActivateStatus.equals(oldActivateStatus)) {
        logger.info(
            "ConfigNode's activation status change: {} -> {}",
            oldActivateStatus,
            nowActivateStatus);
      }
    } else {
      logger.info(
          "ConfigNode's activation status is {}; Previous status is {}",
          nowActivateStatus,
          oldActivateStatus);
    }
    oldActivateStatus = nowActivateStatus;
  }

  private <T> void logFieldDifference(String name, Obligation<T> mine, Obligation<T> another)
      throws LicenseException {
    if (!Objects.equals(mine.getValue(), another.getValue())) {
      String rawContent = String.format("%s: %s -> %s", name, mine.getValue(), another.getValue());
      String encryptedContent = Bandit.publicEncrypt(rawContent);
      logger.info(encryptedContent);
    }
  }

  private void logLicenseDifferences(Lottery anotherLottery) throws LicenseException {
    logFieldDifference(
        "licenseIssueTimestamp", this.licenseIssueTimestamp, anotherLottery.licenseIssueTimestamp);
    logFieldDifference(
        "licenseExpireTimestamp",
        this.licenseExpireTimestamp,
        anotherLottery.licenseExpireTimestamp);
    logFieldDifference(
        "disconnectionFromActiveNodeTimeLimit",
        this.disconnectionFromActiveNodeTimeObligation,
        anotherLottery.disconnectionFromActiveNodeTimeObligation);
    logFieldDifference(
        "skipHardwareSystemInfoCheck",
        this.skipHardwareSystemInfoCheck,
        anotherLottery.skipHardwareSystemInfoCheck);
    logFieldDifference(
        "dataNodeNumLimit", this.dataNodeNumObligation, anotherLottery.dataNodeNumObligation);
    logFieldDifference(
        "dataNodeCpuCoreNumLimit",
        this.dataNodeCpuCoreNumObligation,
        anotherLottery.dataNodeCpuCoreNumObligation);
    logFieldDifference(
        "deviceNumLimit", this.deviceNumObligation, anotherLottery.deviceNumObligation);
    logFieldDifference(
        "sensorNumLimit", this.sensorNumObligation, anotherLottery.sensorNumObligation);
    logFieldDifference(
        "aiNodeNumLimit", this.aiNodeNumObligation, anotherLottery.aiNodeNumObligation);
  }

  // show difference between old license and new license
  private void copyFrom(Lottery anotherLottery) {
    this.licenseIssueTimestamp.setValue(anotherLottery.licenseIssueTimestamp.getValue());
    this.licenseExpireTimestamp.setValue(anotherLottery.licenseExpireTimestamp.getValue());
    this.disconnectionFromActiveNodeTimeObligation.setValue(
        anotherLottery.disconnectionFromActiveNodeTimeObligation.getValue());
    this.skipHardwareSystemInfoCheck.setValue(
        anotherLottery.skipHardwareSystemInfoCheck.getValue());
    this.dataNodeNumObligation.setValue(anotherLottery.dataNodeNumObligation.getValue());
    this.dataNodeCpuCoreNumObligation.setValue(
        anotherLottery.dataNodeCpuCoreNumObligation.getValue());
    this.deviceNumObligation.setValue(anotherLottery.deviceNumObligation.getValue());
    this.sensorNumObligation.setValue(anotherLottery.sensorNumObligation.getValue());
    this.aiNodeNumObligation.setValue(anotherLottery.aiNodeNumObligation.getValue());
  }

  public TLicense toTLicense() {
    return new TLicense(
        this.getLicenseIssueTimestamp(),
        this.getLicenseExpireTimestamp(),
        this.getDataNodeNumLimit(),
        this.getDataNodeCpuCoreNumLimit(),
        this.getDeviceNumLimit(),
        this.getSensorNumLimit(),
        this.getDisconnectionFromActiveNodeTimeLimit(),
        this.getAINodeNumLimit());
  }

  // endregion

  // region show status to outside

  public boolean isActivated() {
    return this.getLicenseExpireTimestamp() >= System.currentTimeMillis();
  }

  public boolean isActive() {
    return LicenseSource.FROM_FILE.equals(licenseSource);
  }

  public boolean noLicense() {
    return LicenseSource.NO_LICENSE.equals(licenseSource);
  }

  /** Only used when detected license file deletion */
  public void licenseFileNotExistOrInvalid() {
    licenseSource = LicenseSource.UNKNOWN;
  }

  public ObligationStatus getActivateStatus() {
    if (this.isActive()) {
      if (this.isActivated()) {
        return ObligationStatus.ACTIVE_ACTIVATED;
      } else {
        return ObligationStatus.ACTIVE_UNACTIVATED;
      }
    } else {
      if (this.isActivated()) {
        return ObligationStatus.PASSIVE_ACTIVATED;
      } else {
        return ObligationStatus.PASSIVE_UNACTIVATED;
      }
    }
  }

  // endregion

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Lottery)) {
      return false;
    }
    Lottery another = (Lottery) obj;
    Iterator<Obligation<?>> iterator = another.allObligation.iterator();
    try {
      for (Obligation<?> obligation : allObligation) {
        if (!Objects.equals(obligation.getValue(), iterator.next().getValue())) {
          return false;
        }
      }
    } catch (Exception e) {
      return false;
    }
    return true;
  }
}
