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
import org.apache.iotdb.db.audit.DNAuditLogger;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.SessionManager;

import com.timecho.iotdb.commons.external.listmatch.PatternList;
import com.timecho.iotdb.i18n.TimechoServerMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

public class IPFilter {
  public static final String IP_LIST_PATTERN =
      "(\\*|25[0-5]|2[0-4]\\d|[0-1]\\d{2}|[1-9]?\\d)\\.(\\*|25[0-5]|2[0-4]\\d|[0-1]\\d{2}|[1-9]?\\d)\\.(\\*|25[0-5]|2[0-4]\\d|[0-1]\\d{2}|[1-9]?\\d)\\.(\\*|25[0-5]|2[0-4]\\d|[0-1]\\d{2}|[1-9]?\\d)";

  private static Logger logger = LoggerFactory.getLogger(IPFilter.class);
  private static IoTDBConfig conf = IoTDBDescriptor.getInstance().getConfig();

  static Set<String> allowListPatterns;

  private IPFilter() {
    throw new UnsupportedOperationException(TimechoServerMessages.CANNOT_INSTANTIATE_THIS_CLASS);
  }

  static PatternList whitePattern;
  static PatternList blackPattern;

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
      return whitePattern.matches(ip);
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
      return blackPattern.matches(ip);
    } finally {
      lock.readLock().unlock();
    }
  }

  public static boolean isDeniedConnect(String ip) {
    loadIPCheckList();
    if (conf.isEnableBlackList() && !conf.isEnableWhiteList()) {
      return isInBlackList(ip);
    } else if (conf.isEnableWhiteList() && !conf.isEnableBlackList()) {
      return !isInWhiteList(ip);
    } else {
      if (isInBlackList(ip)) {
        return true;
      }
      return !isInWhiteList(ip);
    }
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
    if (conf.isEnableWhiteList()) {
      checkValidityOfIP(whiteIPList, true);
    }
    if (conf.isEnableBlackList()) {
      checkValidityOfIP(blackIPList, false);
    }
    allowListPatterns = whiteIPList; // for compatibility
    if (whiteIPList.isEmpty()) {
      whitePattern = null;
      if (blackIPList.isEmpty()) {
        blackPattern = null;
      } else {
        blackPattern = new PatternList(blackIPList, ".", false);
      }
    } else if (blackIPList.isEmpty()) {
      blackPattern = null;
      whitePattern = new PatternList(whiteIPList, ".", false);
    } else {
      whitePattern = new PatternList(whiteIPList, ".", false);
      blackPattern = new PatternList(blackIPList, ".", false);
    }
  }

  private static void checkValidityOfIP(Set<String> ipList, Boolean isWhiteList) {
    Iterator<String> iterator = ipList.iterator();
    Set<String> invalidIps = new HashSet<>();
    String whiteOrBlack = "white";
    while (iterator.hasNext()) {
      String ip = iterator.next();
      if (!Pattern.matches(IP_LIST_PATTERN, ip)) {
        invalidIps.add(ip);
        iterator.remove();
      }
    }
    if (!invalidIps.isEmpty()) {
      if (!isWhiteList) {
        whiteOrBlack = "black";
      }
      logger.error(
          "The IP format configuration for {}list  is incorrect. The detailed information of the incorrect IPs is: {}",
          whiteOrBlack,
          invalidIps.toString());
    }
  }
}
