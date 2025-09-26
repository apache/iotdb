package org.apache.iotdb.db.auth;

import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Deque;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;

public class LoginLockManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(LoginLockManager.class);

  // Configuration parameters
  private final int failedLoginAttempts;
  private final int failedLoginAttemptsPerUser;
  private final int passwordLockTimeMinutes;

  // Lock records storage (in-memory only)
  private final ConcurrentMap<Long, UserLockInfo> userLocks = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, UserLockInfo> userIpLocks = new ConcurrentHashMap<>();

  // Exempt users who should never be locked (only valid if request is from local host)
  private final Set<Long> exemptUsers;

  public LoginLockManager() {
    this(
        IoTDBDescriptor.getInstance().getConfig().getFailedLoginAttempts(),
        IoTDBDescriptor.getInstance().getConfig().getFailedLoginAttemptsPerUser(),
        IoTDBDescriptor.getInstance().getConfig().getPasswordLockTimeMinutes());
  }

  public LoginLockManager(
      int failedLoginAttempts, int failedLoginAttemptsPerUser, int passwordLockTimeMinutes) {
    // Initialize exempt users
    this.exemptUsers = new HashSet<>();
    this.exemptUsers.add(10000L); // root

    // Set and validate failedLoginAttempts (IP level)
    if (failedLoginAttempts == -1) {
      this.failedLoginAttempts = -1; // Completely disable IP-level restrictions
    } else {
      this.failedLoginAttempts = failedLoginAttempts >= 1 ? failedLoginAttempts : 5;
    }

    // Set and validate failedLoginAttemptsPerUser (user level)
    if (failedLoginAttemptsPerUser == -1) {
      // If IP-level is enabled, user-level cannot be disabled
      if (this.failedLoginAttempts != -1) {
        this.failedLoginAttemptsPerUser = 1000; // Default user-level value
        LOGGER.error(
            "User-level login attempts cannot be disabled when IP-level is enabled. "
                + "Setting user-level attempts to default (1000)");
      } else {
        this.failedLoginAttemptsPerUser = -1; // Both are disabled
      }
    } else {
      this.failedLoginAttemptsPerUser =
          failedLoginAttemptsPerUser >= 1 ? failedLoginAttemptsPerUser : 1000;
    }

    // Set and validate passwordLockTimeMinutes (default 10, minimum 1)
    this.passwordLockTimeMinutes = passwordLockTimeMinutes >= 1 ? passwordLockTimeMinutes : 10;

    // Log final effective configuration
    LOGGER.info(
        "Login lock manager initialized with: IP-level attempts={}, User-level attempts={}, Lock time={} minutes",
        this.failedLoginAttempts == -1 ? "disabled" : this.failedLoginAttempts,
        this.failedLoginAttemptsPerUser == -1 ? "disabled" : this.failedLoginAttemptsPerUser,
        this.passwordLockTimeMinutes);
  }

  /** Inner class to store user lock information */
  static class UserLockInfo {
    // Deque to store timestamps of failed attempts (milliseconds)
    private final Deque<Long> failureTimestamps = new ConcurrentLinkedDeque<>();

    void addFailureTime(long timestamp) {
      failureTimestamps.addLast(timestamp);
    }

    void removeOldFailures(long cutoffTime) {
      // Remove timestamps older than cutoffTime
      while (!failureTimestamps.isEmpty() && failureTimestamps.peekFirst() < cutoffTime) {
        failureTimestamps.pollFirst();
      }
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
    if (exemptUsers.contains(userId) && isFromLocalhost(ip)) {
      return false;
    }

    // Check user@ip lock (failures in window)
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

    // Check global user lock (failures in window)
    UserLockInfo userLock = userLocks.get(userId);
    if (userLock != null) {
      long now = System.currentTimeMillis();
      long cutoffTime = now - (passwordLockTimeMinutes * 60 * 1000L);
      userLock.removeOldFailures(cutoffTime);
      return userLock.getFailureCount() >= failedLoginAttemptsPerUser;
    }

    return false;
  }

  /**
   * Record a failed login attempt
   *
   * @param userId user ID
   * @param ip IP address
   */
  public void recordFailure(long userId, String ip) {
    // Exempt users from localhost don't get locked
    if (exemptUsers.contains(userId) && isFromLocalhost(ip)) {
      return;
    }

    long now = System.currentTimeMillis();
    long cutoffTime = now - (passwordLockTimeMinutes * 60 * 1000L);

    // Handle user@ip failures in sliding window
    String userIpKey = buildUserIpKey(userId, ip);
    userIpLocks.compute(
        userIpKey,
        (key, existing) -> {
          if (existing == null) {
            existing = new UserLockInfo();
          }
          // Remove failures outside of sliding window
          existing.removeOldFailures(cutoffTime);
          // Record this failure
          existing.addFailureTime(now);
          // Check if threshold reached (log only when it just reaches)
          int failCountIp = existing.getFailureCount();
          if (failCountIp >= failedLoginAttempts && failCountIp == failedLoginAttempts) {
            LOGGER.info("IP '{}' locked for user ID '{}'", ip, userId);
          }
          return existing;
        });

    // Handle global user failures in sliding window
    userLocks.compute(
        userId,
        (key, existing) -> {
          if (existing == null) {
            existing = new UserLockInfo();
          }
          // Remove failures outside of sliding window
          existing.removeOldFailures(cutoffTime);
          // Record this failure
          existing.addFailureTime(now);
          // Check if threshold reached (log only when it just reaches)
          int failCountUser = existing.getFailureCount();
          if (failCountUser >= failedLoginAttemptsPerUser
              && failCountUser == failedLoginAttemptsPerUser) {
            LOGGER.info(
                "User ID '{}' locked due to {} failed attempts",
                userId,
                failedLoginAttemptsPerUser);
          }
          return existing;
        });

    // Check for potential attacks
    checkForPotentialAttacks(userId, ip);
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
  }

  /**
   * Unlock user or user@ip
   *
   * @param userId user ID (required)
   * @param ip IP address (optional)
   */
  public void unlock(long userId, String ip) {
    if (ip == null || ip.isEmpty()) {
      // Unlock global user lock
      userLocks.remove(userId);
      // Also remove all IP locks for this user
      userIpLocks.keySet().removeIf(key -> key.startsWith(userId + "@"));
      LOGGER.info("User ID '{}' unlocked (manual)", userId);
    } else {
      // Unlock specific user@ip lock
      String userIpKey = buildUserIpKey(userId, ip);
      userIpLocks.remove(userIpKey);
      LOGGER.info("IP '{}' for user ID '{}' unlocked (manual)", ip, userId);
    }
  }

  /** Clean up expired locks (no failures in the sliding window) */
  public void cleanExpiredLocks() {
    long now = System.currentTimeMillis();
    long cutoffTime = now - (passwordLockTimeMinutes * 60 * 1000L);

    // Clean expired user locks
    userLocks
        .entrySet()
        .removeIf(
            entry -> {
              UserLockInfo info = entry.getValue();
              // Remove outdated failures
              info.removeOldFailures(cutoffTime);
              if (info.getFailureCount() == 0) {
                LOGGER.info("User ID '{}' unlocked (expired)", entry.getKey());
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
                String[] parts = entry.getKey().split("@");
                LOGGER.info("IP '{}' for user ID '{}' unlocked (expired)", parts[1], parts[0]);
                return true;
              }
              return false;
            });
  }

  // Helper methods
  private String buildUserIpKey(long userId, String ip) {
    return userId + "@" + ip;
  }

  private void checkForPotentialAttacks(long userId, String ip) {
    // Check if IP is locked by many users
    Set<Long> usersForIp = new HashSet<>();
    for (String key : userIpLocks.keySet()) {
      if (key.endsWith("@" + ip)) {
        usersForIp.add(Long.parseLong(key.split("@")[0]));
      }
    }

    if (usersForIp.size() > 50) {
      LOGGER.warn("IP '{}' locked by {} different users → potential attack", ip, usersForIp.size());
    }

    // Check if user has many IP locks
    Set<String> ipsForUser = new HashSet<>();
    for (String key : userIpLocks.keySet()) {
      if (key.startsWith(userId + "@")) {
        ipsForUser.add(key.split("@")[1]);
      }
    }

    if (ipsForUser.size() > 100) {
      LOGGER.warn("User ID '{}' has {} IP locks → potential attack", userId, ipsForUser.size());
    }
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
      return false; // In case of error, assume non-local
    }
    return false;
  }
}
