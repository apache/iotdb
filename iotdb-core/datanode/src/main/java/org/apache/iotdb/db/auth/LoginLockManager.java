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

package org.apache.iotdb.db.auth;

import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Collections;
import java.util.Deque;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;

public class LoginLockManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(LoginLockManager.class);
  private static final int MULTIPLE_USERS_FOR_IP_WARNING_THRESHOLD = 50;
  private static final int MULTIPLE_IPS_FOR_USER_WARNING_THRESHOLD = 100;

  // Configuration parameters
  private final int failedLoginAttempts;
  private int failedLoginAttemptsPerUser;
  private final int passwordLockTimeMinutes;

  // Lock records storage (in-memory only)
  private final ConcurrentMap<Long, UserLockInfo> userLocks = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, UserLockInfo> userIpLocks = new ConcurrentHashMap<>();
  private final Set<String> warnedIpsWithMultipleUsers = ConcurrentHashMap.newKeySet();
  private final Set<Long> warnedUsersWithMultipleIpLocks = ConcurrentHashMap.newKeySet();

  // Exempt users who should never be locked (only valid if request is from local host)
  private static final Set<Long> EXEMPT_USERS;

  static {
    Set<Long> tempSet = new HashSet<>();
    tempSet.add((long) AuthorityChecker.SUPER_USER_ID); // root userid
    tempSet.add(User.INTERNAL_SECURITY_ADMIN);
    EXEMPT_USERS = Collections.unmodifiableSet(tempSet);
  }

  public LoginLockManager() {
    this(
        IoTDBDescriptor.getInstance().getConfig().getFailedLoginAttempts(),
        IoTDBDescriptor.getInstance().getConfig().getFailedLoginAttemptsPerUser(),
        IoTDBDescriptor.getInstance().getConfig().getPasswordLockTimeMinutes());
  }

  public LoginLockManager(
      int failedLoginAttempts, int failedLoginAttemptsPerUser, int passwordLockTimeMinutes) {
    // Set and validate failedLoginAttempts (IP level)
    if (failedLoginAttempts <= 0) {
      this.failedLoginAttempts = -1; // Completely disable IP-level restrictions
      LOGGER.info(DataNodeMiscMessages.IP_LOGIN_ATTEMPTS_DISABLED, failedLoginAttempts);
    } else {
      this.failedLoginAttempts = failedLoginAttempts;
    }

    // Set and validate failedLoginAttemptsPerUser (user level)
    if (failedLoginAttemptsPerUser <= 0) {
      this.failedLoginAttemptsPerUser = -1; // Disable user-level restrictions
      LOGGER.info(DataNodeMiscMessages.USER_LOGIN_ATTEMPTS_DISABLED, failedLoginAttemptsPerUser);

      // Additional check: if IP-level is enabled (>1), enable user-level with default 1000
      if (this.failedLoginAttempts > 1) {
        this.failedLoginAttemptsPerUser = 1000;
        LOGGER.warn(
            DataNodeMiscMessages
                .MISC_LOG_USER_LEVEL_ATTEMPTS_AUTO_ENABLED_WITH_DEFAULT_1000_BECAUSE_FAB86B7D,
            this.failedLoginAttempts);
      }
    } else {
      this.failedLoginAttemptsPerUser = failedLoginAttemptsPerUser;
    }

    // Set and validate passwordLockTimeMinutes (default 10, minimum 1)
    this.passwordLockTimeMinutes = passwordLockTimeMinutes >= 1 ? passwordLockTimeMinutes : 10;
    if (passwordLockTimeMinutes < 1) {
      LOGGER.warn(
          DataNodeMiscMessages
              .MISC_LOG_INVALID_LOCK_TIME_VALUE_RESET_TO_DEFAULT_10_MINUTES_8DCE21EF,
          passwordLockTimeMinutes);
    }

    // Log final effective configuration
    LOGGER.info(
        DataNodeMiscMessages
            .MISC_LOG_LOGIN_LOCK_MANAGER_INITIALIZED_WITH_IP_LEVEL_ATTEMPTS_USER_57AE7966,
        this.failedLoginAttempts == -1 ? "disabled" : this.failedLoginAttempts,
        this.failedLoginAttemptsPerUser == -1 ? "disabled" : this.failedLoginAttemptsPerUser,
        this.passwordLockTimeMinutes);
  }

  /** Inner class to store user lock information */
  static class UserLockInfo {

    // Deque to store timestamps of failed attempts (milliseconds)
    private final Deque<Long> failureTimestamps;

    UserLockInfo(int capacity) {
      failureTimestamps = new ConcurrentLinkedDeque<>();
    }

    synchronized void addFailureTime(long timestamp) {
      failureTimestamps.addLast(timestamp);
    }

    synchronized void removeOldFailures(long cutoffTime) {
      // Remove timestamps older than cutoffTime
      failureTimestamps.removeIf(timestamp -> timestamp < cutoffTime);
    }

    int getFailureCount() {
      return failureTimestamps.size();
    }
  }

  /**
   * Check if user or user@ip is locked
   *
   * @param userId user ID
   * @param ip IP address
   * @return true if locked, false otherwise
   */
  public boolean checkLock(long userId, String ip) {
    cleanExpiredLocks(); // Clean expired records (no failures in window)

    // Exempt users are never locked if request is from localhost
    if (EXEMPT_USERS.contains(userId) && isFromLocalhost(ip)) {
      return false;
    }

    // Check user@ip lock (failures in window)
    if (failedLoginAttempts != -1) {
      String userIpKey = buildUserIpKey(userId, ip);
      UserLockInfo userIpLock = userIpLocks.get(userIpKey);
      if (userIpLock != null) {
        long now = System.currentTimeMillis();
        long cutoffTime = now - (passwordLockTimeMinutes * 60 * 1000L);
        userIpLock.removeOldFailures(cutoffTime);
        if (userIpLock.getFailureCount() >= failedLoginAttempts) {
          return true;
        }
      }
    }

    // Check global user lock (failures in window)
    if (failedLoginAttemptsPerUser != -1) {
      UserLockInfo userLock = userLocks.get(userId);
      if (userLock != null) {
        long now = System.currentTimeMillis();
        long cutoffTime = now - (passwordLockTimeMinutes * 60 * 1000L);
        userLock.removeOldFailures(cutoffTime);
        return userLock.getFailureCount() >= failedLoginAttemptsPerUser;
      }
    }

    return false;
  }

  /**
   * Returns the number of consecutive failed login attempts.
   *
   * @return the number of failed login attempts
   */
  public int getFailedLoginAttempts() {
    return failedLoginAttempts;
  }

  /**
   * Record a failed login attempt
   *
   * @param userId user ID
   * @param ip IP address
   */
  public void recordFailure(long userId, String ip) {
    // Exempt users from localhost don't get locked
    if (EXEMPT_USERS.contains(userId) && isFromLocalhost(ip)) {
      return;
    }

    long now = System.currentTimeMillis();
    long cutoffTime = now - (passwordLockTimeMinutes * 60 * 1000L);

    // Handle user@ip failures in sliding window
    if (failedLoginAttempts != -1) {
      String userIpKey = buildUserIpKey(userId, ip);
      userIpLocks.compute(
          userIpKey,
          (key, existing) -> {
            if (existing == null) {
              existing =
                  new UserLockInfo(Math.max(failedLoginAttempts, failedLoginAttemptsPerUser));
            }
            // Remove failures outside of sliding window
            existing.removeOldFailures(cutoffTime);
            // Record this failure
            existing.addFailureTime(now);
            // Check if threshold reached (log only when it just reaches)
            int failCountIp = existing.getFailureCount();
            if (failCountIp == failedLoginAttempts) {
              LOGGER.info(DataNodeMiscMessages.IP_LOCKED, ip, userId);
            }
            return existing;
          });
    }

    // Handle global user failures in sliding window
    if (failedLoginAttemptsPerUser != -1) {
      userLocks.compute(
          userId,
          (key, existing) -> {
            if (existing == null) {
              existing =
                  new UserLockInfo(Math.max(failedLoginAttempts, failedLoginAttemptsPerUser));
            }
            // Remove failures outside of sliding window
            existing.removeOldFailures(cutoffTime);
            // Record this failure
            existing.addFailureTime(now);
            // Check if threshold reached (log only when it just reaches)
            int failCountUser = existing.getFailureCount();
            if (failCountUser == failedLoginAttemptsPerUser) {
              LOGGER.info(
                  DataNodeMiscMessages.MISC_LOG_USER_ID_LOCKED_DUE_TO_FAILED_ATTEMPTS_743CFB3A,
                  userId,
                  failedLoginAttemptsPerUser);
            }
            return existing;
          });
    }

    // Check for potential attacks
    if (failedLoginAttempts != -1 || failedLoginAttemptsPerUser != -1) {
      checkForPotentialAttacks(userId, ip);
    }
  }

  /**
   * Clear failure records after successful login
   *
   * @param userId user ID
   * @param ip IP address
   */
  public void clearFailure(long userId, String ip) {
    String userIpKey = buildUserIpKey(userId, ip);
    userIpLocks.remove(userIpKey);
    userLocks.remove(userId);
    resetPotentialAttackWarningsIfBelowThreshold(userId, ip);
  }

  /**
   * Unlock user or user@ip
   *
   * @param userId user ID (required)
   * @param ip IP address (optional)
   */
  public void unlock(long userId, String ip) {
    if (ip == null || ip.isEmpty()) {
      Set<String> affectedIps = new HashSet<>();
      for (String key : userIpLocks.keySet()) {
        if (key.startsWith(userId + "@")) {
          String[] parts = key.split("@", 2);
          if (parts.length == 2) {
            affectedIps.add(parts[1]);
          }
        }
      }
      // Unlock global user lock
      userLocks.remove(userId);
      // Also remove all IP locks for this user
      userIpLocks.keySet().removeIf(key -> key.startsWith(userId + "@"));
      warnedUsersWithMultipleIpLocks.remove(userId);
      affectedIps.forEach(this::resetIpWarningIfBelowThreshold);
      LOGGER.info(DataNodeMiscMessages.USER_UNLOCKED_MANUAL, userId);
    } else {
      // Unlock specific user@ip lock
      String userIpKey = buildUserIpKey(userId, ip);
      userIpLocks.remove(userIpKey);
      resetPotentialAttackWarningsIfBelowThreshold(userId, ip);
      LOGGER.info(DataNodeMiscMessages.IP_UNLOCKED_MANUAL, ip, userId);
    }
  }

  /** Clean up expired locks (no failures in the sliding window) */
  public void cleanExpiredLocks() {
    long now = System.currentTimeMillis();
    long cutoffTime = now - (passwordLockTimeMinutes * 60 * 1000L);
    Set<Long> affectedUsers = new HashSet<>();
    Set<String> affectedIps = new HashSet<>();

    // Clean expired user locks
    userLocks
        .entrySet()
        .removeIf(
            entry -> {
              UserLockInfo info = entry.getValue();
              // Remove outdated failures
              info.removeOldFailures(cutoffTime);
              if (info.getFailureCount() == 0) {
                LOGGER.info(DataNodeMiscMessages.USER_UNLOCKED_EXPIRED, entry.getKey());
                return true;
              }
              return false;
            });

    // Clean expired user@ip locks
    userIpLocks
        .entrySet()
        .removeIf(
            entry -> {
              UserLockInfo info = entry.getValue();
              // Remove outdated failures
              info.removeOldFailures(cutoffTime);
              if (info.getFailureCount() == 0) {
                final String[] parts = entry.getKey().split("@", 2);
                if (parts.length == 2) {
                  affectedUsers.add(Long.parseLong(parts[0]));
                  affectedIps.add(parts[1]);
                }
                LOGGER.info(
                    DataNodeMiscMessages.IP_UNLOCKED_EXPIRED,
                    parts.length == 2 ? parts[1] : "",
                    parts.length >= 1 ? parts[0] : "");
                return true;
              }
              return false;
            });

    affectedUsers.forEach(this::resetUserWarningIfBelowThreshold);
    affectedIps.forEach(this::resetIpWarningIfBelowThreshold);
  }

  // Helper methods
  private String buildUserIpKey(long userId, String ip) {
    return userId + "@" + ip;
  }

  private void checkForPotentialAttacks(long userId, String ip) {
    // Check if IP is locked by many users
    if (ip != null && !ip.isEmpty()) {
      Set<Long> usersForIp = new HashSet<>();
      for (String key : userIpLocks.keySet()) {
        if (key.endsWith("@" + ip)) {
          usersForIp.add(Long.parseLong(key.split("@")[0]));
        }
      }

      if (usersForIp.size() > MULTIPLE_USERS_FOR_IP_WARNING_THRESHOLD) {
        if (warnedIpsWithMultipleUsers.add(ip)) {
          LOGGER.warn(DataNodeMiscMessages.IP_LOCKED_MULTIPLE_USERS, ip, usersForIp.size());
        }
      } else {
        warnedIpsWithMultipleUsers.remove(ip);
      }
    }

    // Check if user has many IP locks
    Set<String> ipsForUser = new HashSet<>();
    for (String key : userIpLocks.keySet()) {
      if (key.startsWith(userId + "@")) {
        ipsForUser.add(key.split("@")[1]);
      }
    }

    if (ipsForUser.size() > MULTIPLE_IPS_FOR_USER_WARNING_THRESHOLD) {
      if (warnedUsersWithMultipleIpLocks.add(userId)) {
        LOGGER.warn(DataNodeMiscMessages.USER_MULTIPLE_IP_LOCKS, userId, ipsForUser.size());
      }
    } else {
      warnedUsersWithMultipleIpLocks.remove(userId);
    }
  }

  private void resetPotentialAttackWarningsIfBelowThreshold(long userId, String ip) {
    resetUserWarningIfBelowThreshold(userId);
    if (ip != null && !ip.isEmpty()) {
      resetIpWarningIfBelowThreshold(ip);
    }
  }

  private void resetUserWarningIfBelowThreshold(long userId) {
    if (countIpsForUser(userId) <= MULTIPLE_IPS_FOR_USER_WARNING_THRESHOLD) {
      warnedUsersWithMultipleIpLocks.remove(userId);
    }
  }

  private void resetIpWarningIfBelowThreshold(String ip) {
    if (countUsersForIp(ip) <= MULTIPLE_USERS_FOR_IP_WARNING_THRESHOLD) {
      warnedIpsWithMultipleUsers.remove(ip);
    }
  }

  private int countUsersForIp(String ip) {
    Set<Long> usersForIp = new HashSet<>();
    for (String key : userIpLocks.keySet()) {
      if (key.endsWith("@" + ip)) {
        usersForIp.add(Long.parseLong(key.split("@")[0]));
      }
    }
    return usersForIp.size();
  }

  private int countIpsForUser(long userId) {
    Set<String> ipsForUser = new HashSet<>();
    for (String key : userIpLocks.keySet()) {
      if (key.startsWith(userId + "@")) {
        ipsForUser.add(key.split("@")[1]);
      }
    }
    return ipsForUser.size();
  }

  public static LoginLockManager getInstance() {
    return LoginLockManagerHelper.INSTANCE;
  }

  private static class LoginLockManagerHelper {
    private static final LoginLockManager INSTANCE = new LoginLockManager();

    private LoginLockManagerHelper() {}
  }

  /**
   * Check if an IP address belongs to localhost (loopback or any local network interface).
   *
   * @param ip The IP address as string.
   * @return true if the IP is local, false otherwise. Note: Network interface addresses are
   *     reacquired each time to account for possible address changes.
   */
  private boolean isFromLocalhost(String ip) {
    try {
      if (ip == null || ip.isEmpty()) {
        return false;
      }
      InetAddress remote = InetAddress.getByName(ip);

      // Case 1: Explicit loopback address (127.0.0.1 or ::1)
      if (remote.isLoopbackAddress()) {
        return true;
      }

      // Case 2: Compare against all local network interface addresses
      Enumeration<NetworkInterface> nics = NetworkInterface.getNetworkInterfaces();
      while (nics.hasMoreElements()) {
        NetworkInterface nic = nics.nextElement();
        if (!nic.isUp()) {
          continue; // Skip inactive interfaces
        }
        Enumeration<InetAddress> addrs = nic.getInetAddresses();
        while (addrs.hasMoreElements()) {
          InetAddress localAddr = addrs.nextElement();
          if (remote.equals(localAddr)) {
            return true; // Remote address matches one of the local addresses
          }
        }
      }
    } catch (Exception e) {
      LOGGER.warn(DataNodeMiscMessages.FAILED_CHECK_IP_UP, ip, e);
      return false; // In case of error, assume non-local
    }
    return false;
  }
}
