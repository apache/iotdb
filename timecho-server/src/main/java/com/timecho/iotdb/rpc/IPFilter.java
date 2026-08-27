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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.timecho.iotdb.rpc;

import org.apache.iotdb.commons.audit.AuditEventType;
import org.apache.iotdb.commons.audit.AuditLogFields;
import org.apache.iotdb.commons.audit.AuditLogOperation;
import org.apache.iotdb.commons.conf.EditionGate;
import org.apache.iotdb.db.audit.DNAuditLogger;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.SessionManager;

import com.timecho.iotdb.i18n.TimechoServerMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class IPFilter {
  public static final String IP_LIST_PATTERN =
      "(\\*|25[0-5]|2[0-4]\\d|[0-1]\\d{2}|[1-9]?\\d)\\.(\\*|25[0-5]|2[0-4]\\d|[0-1]\\d{2}|[1-9]?\\d)\\.(\\*|25[0-5]|2[0-4]\\d|[0-1]\\d{2}|[1-9]?\\d)\\.(\\*|25[0-5]|2[0-4]\\d|[0-1]\\d{2}|[1-9]?\\d)";

  private static Logger logger = LoggerFactory.getLogger(IPFilter.class);
  private static IoTDBConfig conf = IoTDBDescriptor.getInstance().getConfig();

  static Set<String> allowListPatterns;

  private IPFilter() {
    throw new UnsupportedOperationException(TimechoServerMessages.CANNOT_INSTANTIATE_THIS_CLASS);
  }

  static volatile IPMatcher whitePattern;
  static volatile IPMatcher blackPattern;

  static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  static {
    logger.info(TimechoServerMessages.INITIALIZING_WHITE_BLACK_LIST_UPDATE_CALLBACK);

    Runnable updateSessionCallback =
        () -> {
          if (!conf.isEnableBlackList() && !conf.isEnableWhiteList()) {
            return;
          }
          SessionManager.getInstance()
              .removeSessions(
                  session -> {
                    String clientAddress = session.getClientAddress();
                    boolean shouldRemove = isDeniedConnect(clientAddress);
                    if (shouldRemove) {
                      DNAuditLogger.getInstance()
                          .log(
                              new AuditLogFields(
                                  session.getUserId(),
                                  session.getUsername(),
                                  session.getClientAddress(),
                                  AuditEventType.CONNECTION_EVICTED,
                                  AuditLogOperation.CONTROL,
                                  false),
                              () ->
                                  String.format(
                                      "User %s (ID=%d) connection evicted. ",
                                      session.getUsername(), session.getUserId()));
                    }
                    return shouldRemove;
                  });
        };

    conf.setOnBlackListUpdated(updateSessionCallback);
    conf.setOnWhiteListUpdated(updateSessionCallback);
  }

  public static boolean isInWhiteList(String ip) {
    if (whitePattern == null) {
      return false;
    }
    lock.readLock().lock();
    try {
      return whitePattern != null && whitePattern.matches(ip);
    } finally {
      lock.readLock().unlock();
    }
  }

  public static boolean isInBlackList(String ip) {
    if (blackPattern == null) {
      return false;
    }
    lock.readLock().lock();
    try {
      return blackPattern != null && blackPattern.matches(ip);
    } finally {
      lock.readLock().unlock();
    }
  }

  public static boolean isDeniedConnect(String ip) {
    if (EditionGate.isIoTDB()) {
      return false;
    }
    loadIPCheckList();
    if (conf.isEnableBlackList() && !conf.isEnableWhiteList()) {
      return isInBlackList(ip);
    } else if (conf.isEnableWhiteList() && !conf.isEnableBlackList()) {
      return !isInWhiteList(ip);
    } else if (conf.isEnableWhiteList() && conf.isEnableBlackList()) {
      if (isInBlackList(ip)) {
        return true;
      }
      return !isInWhiteList(ip);
    }
    return false;
  }

  public static Set<String> getAllowListPatterns() {
    return allowListPatterns;
  }

  private static void loadIPCheckList() {
    Set<String> whiteIPList =
        Objects.equals(conf.getRawWhiteIPList(), "")
            ? Collections.emptySet()
            : new HashSet<>(
                Arrays.asList(
                    Arrays.stream(conf.getRawWhiteIPList().split(","))
                        .map(String::trim)
                        .toArray(String[]::new)));
    Set<String> blackIPList =
        Objects.equals(conf.getRawBlackIPList(), "")
            ? Collections.emptySet()
            : new HashSet<>(
                Arrays.asList(
                    Arrays.stream(conf.getRawBlackIPList().split(","))
                        .map(String::trim)
                        .toArray(String[]::new)));
    IPMatcher.BuildResult whiteResult =
        conf.isEnableWhiteList() ? IPMatcher.build(whiteIPList) : null;
    IPMatcher.BuildResult blackResult =
        conf.isEnableBlackList() ? IPMatcher.build(blackIPList) : null;
    Set<String> validWhiteIPList = new LinkedHashSet<>(whiteIPList);
    if (whiteResult != null) {
      validWhiteIPList.removeAll(whiteResult.getInvalidPatterns());
    }

    lock.writeLock().lock();
    try {
      whitePattern =
          whiteResult == null || whiteResult.getMatcher().isEmpty()
              ? null
              : whiteResult.getMatcher();
      blackPattern =
          blackResult == null || blackResult.getMatcher().isEmpty()
              ? null
              : blackResult.getMatcher();
      allowListPatterns =
          whiteResult == null
              ? Collections.emptySet()
              : Collections.unmodifiableSet(validWhiteIPList);
    } finally {
      lock.writeLock().unlock();
    }

    if (whiteResult != null) {
      logInvalidPatterns(whiteResult.getInvalidPatterns(), true);
    }
    if (blackResult != null) {
      logInvalidPatterns(blackResult.getInvalidPatterns(), false);
    }
  }

  private static void logInvalidPatterns(Set<String> invalidPatterns, boolean isWhiteList) {
    if (invalidPatterns.isEmpty()) {
      return;
    }
    String whiteOrBlack = "white";
    if (!isWhiteList) {
      whiteOrBlack = "black";
    }
    logger.error(
        TimechoServerMessages
            .LOG_THE_IP_FORMAT_CONFIGURATION_FOR_ARG_LIST_IS_INCORRECT_THE_DETAILED_INFORMATION_OF_THE_INCORRECT_IPS_IS_ARG_8649B43F,
        whiteOrBlack,
        invalidPatterns);
  }
}
