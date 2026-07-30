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

import org.apache.iotdb.db.auth.LoginLockManager.UserLockInfo;

import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LoginLockManagerTest {

  private LoginLockManager lockManager;
  private static final long TEST_USER_ID = 1001L;
  private static final long OTHER_USER_ID = 2002L;
  private static final String TEST_IP = "192.168.1.1";
  private static final String ANOTHER_IP = "10.0.0.1";
  private static final long EXEMPT_USER_ID = 0L; // root

  private final int failedLoginAttempts = 3;
  private final int failedLoginAttemptsPerUser = 5;
  private final int passwordLockTimeMinutes = 1;

  @Before
  public void setUp() {
    lockManager =
        new LoginLockManager(
            failedLoginAttempts, failedLoginAttemptsPerUser, passwordLockTimeMinutes);
  }

  // -------------------- Configuration --------------------
  @Test
  public void testAllConfigScenarios() {
    // 1. Test normal configuration with recommended defaults
    LoginLockManager normal = new LoginLockManager(5, 1000, 10);
    assertEquals(5, getField(normal, "failedLoginAttempts"));
    assertEquals(1000, getField(normal, "failedLoginAttemptsPerUser"));
    assertEquals(10, getField(normal, "passwordLockTimeMinutes"));

    // 2. Test disabling both IP-level and user-level restrictions (using -1 or 0)
    LoginLockManager disableBoth1 = new LoginLockManager(-1, -1, 10);
    assertEquals(-1, getField(disableBoth1, "failedLoginAttempts"));
    assertEquals(-1, getField(disableBoth1, "failedLoginAttemptsPerUser"));

    LoginLockManager disableBoth2 = new LoginLockManager(0, 0, 10);
    assertEquals(-1, getField(disableBoth2, "failedLoginAttempts"));
    assertEquals(-1, getField(disableBoth2, "failedLoginAttemptsPerUser"));

    // 3. Test mixed scenarios (IP enabled + user disabled, and vice versa)
    LoginLockManager ipEnabledUserDisabled = new LoginLockManager(3, 0, 10);
    assertEquals(3, getField(ipEnabledUserDisabled, "failedLoginAttempts"));
    assertEquals(1000, getField(ipEnabledUserDisabled, "failedLoginAttemptsPerUser"));

    LoginLockManager ipDisabledUserEnabled = new LoginLockManager(-1, 5, 10);
    assertEquals(-1, getField(ipDisabledUserEnabled, "failedLoginAttempts"));
    assertEquals(5, getField(ipDisabledUserEnabled, "failedLoginAttemptsPerUser"));

    // 4. Test invalid positive values fall back to defaults
    LoginLockManager invalidIp =
        new LoginLockManager(-5, 1000, 10); // Negative treated as disable (-1)
    assertEquals(-1, getField(invalidIp, "failedLoginAttempts"));

    LoginLockManager invalidUser =
        new LoginLockManager(5, -2, 10); // Negative treated as disable (-1)
    assertEquals(1000, getField(invalidUser, "failedLoginAttemptsPerUser"));

    // 5. Test lock time validation
    LoginLockManager zeroLockTime = new LoginLockManager(5, 1000, 0);
    assertEquals(10, getField(zeroLockTime, "passwordLockTimeMinutes"));

    LoginLockManager negativeLockTime = new LoginLockManager(5, 1000, -5);
    assertEquals(10, getField(negativeLockTime, "passwordLockTimeMinutes"));

    LoginLockManager customLockTime = new LoginLockManager(5, 1000, 30);
    assertEquals(30, getField(customLockTime, "passwordLockTimeMinutes"));
  }

  private int getField(LoginLockManager manager, String fieldName) {
    try {
      java.lang.reflect.Field field = LoginLockManager.class.getDeclaredField(fieldName);
      field.setAccessible(true);
      return (int) field.get(manager);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // ---------------- Basic Functionality ----------------

  @Test
  public void testInitialStateNotLocked() {
    assertFalse("New user should not be locked", lockManager.checkLock(TEST_USER_ID, TEST_IP));
  }

  @Test
  public void testSingleFailureNotLocked() {
    lockManager.recordFailure(TEST_USER_ID, TEST_IP);
    assertFalse("Single failure should not lock", lockManager.checkLock(TEST_USER_ID, TEST_IP));
  }

  @Test
  public void testIpLockAfterMaxAttempts() {
    for (int i = 0; i < failedLoginAttempts; i++) {
      lockManager.recordFailure(TEST_USER_ID, TEST_IP);
    }
    assertTrue(
        "User@IP should be locked after max attempts",
        lockManager.checkLock(TEST_USER_ID, TEST_IP));
  }

  @Test
  public void testGlobalUserLockAfterMaxAttempts() {
    for (int i = 0; i < failedLoginAttemptsPerUser; i++) {
      lockManager.recordFailure(TEST_USER_ID, "ip" + i);
    }
    assertTrue(
        "User should be locked globally after max attempts",
        lockManager.checkLock(TEST_USER_ID, TEST_IP));
  }

  @Test
  public void testExemptUsersLocalIpNeverLocked() throws Exception {
    // Use loopback as local IP
    String localIp = InetAddress.getLoopbackAddress().getHostAddress();

    for (int i = 0; i < 20; i++) {
      lockManager.recordFailure(EXEMPT_USER_ID, localIp);
    }
    assertFalse(
        "Exempt user with local IP should never be locked",
        lockManager.checkLock(EXEMPT_USER_ID, localIp));
  }

  @Test
  public void testExemptUsersRemoteIpCanBeLocked() {
    for (int i = 0; i < failedLoginAttempts; i++) {
      lockManager.recordFailure(EXEMPT_USER_ID, TEST_IP);
    }
    assertTrue(
        "Exempt user with non-local IP should still be locked",
        lockManager.checkLock(EXEMPT_USER_ID, TEST_IP));
  }

  // -------------------- User-Level Locking --------------------
  @Test
  public void testUserLevelOLocking() {
    LoginLockManager userOnlyManager = new LoginLockManager(-1, 10, 1);

    for (int i = 0; i < 10; i++) {
      userOnlyManager.recordFailure(TEST_USER_ID, "ip" + i); // Different IPs each time
    }
    // Should be locked
    assertTrue(userOnlyManager.checkLock(TEST_USER_ID, null));
    for (int i = 0; i < 10; i++) {
      assertTrue(userOnlyManager.checkLock(TEST_USER_ID, "ip" + i)); // Different IPs each time
    }
  }

  // ---------------- Unlock Logic ----------------

  @Test
  public void testManualUnlock() {
    for (int i = 0; i < failedLoginAttempts; i++) {
      lockManager.recordFailure(TEST_USER_ID, TEST_IP);
    }
    assertTrue(lockManager.checkLock(TEST_USER_ID, TEST_IP));

    lockManager.unlock(TEST_USER_ID, TEST_IP);
    assertFalse(
        "User should be unlocked after manual unlock",
        lockManager.checkLock(TEST_USER_ID, TEST_IP));
  }

  @Test
  public void testGlobalManualUnlock() {
    for (int i = 0; i < failedLoginAttemptsPerUser; i++) {
      lockManager.recordFailure(TEST_USER_ID, "ip" + i);
    }
    assertTrue(lockManager.checkLock(TEST_USER_ID, TEST_IP));

    lockManager.unlock(TEST_USER_ID, null);
    assertFalse(
        "User should be unlocked after manual global unlock",
        lockManager.checkLock(TEST_USER_ID, TEST_IP));
  }

  @Test
  public void testClearFailureResetsCounters() {
    lockManager.recordFailure(TEST_USER_ID, TEST_IP);
    lockManager.recordFailure(TEST_USER_ID, TEST_IP);

    lockManager.clearFailure(TEST_USER_ID, TEST_IP);

    for (int i = 0; i < failedLoginAttempts - 1; i++) {
      lockManager.recordFailure(TEST_USER_ID, TEST_IP);
    }
    assertFalse(
        "Counters should be reset after clear", lockManager.checkLock(TEST_USER_ID, TEST_IP));
  }

  @Test
  public void testEmptyIpInUnlock() {
    lockManager.unlock(TEST_USER_ID, "");
    // no exception expected
  }

  // ---------------- Multi-user Multi-IP ----------------

  @Test
  public void testDifferentUsersAreIndependent() {
    for (int i = 0; i < failedLoginAttempts; i++) {
      lockManager.recordFailure(TEST_USER_ID, TEST_IP);
    }
    assertTrue(lockManager.checkLock(TEST_USER_ID, TEST_IP));
    assertFalse("Other user should not be locked", lockManager.checkLock(OTHER_USER_ID, TEST_IP));
  }

  @Test
  public void testDifferentIpsCountSeparately() {
    for (int i = 0; i < failedLoginAttempts - 1; i++) {
      lockManager.recordFailure(TEST_USER_ID, TEST_IP);
    }
    for (int i = 0; i < failedLoginAttempts - 1; i++) {
      lockManager.recordFailure(TEST_USER_ID, ANOTHER_IP);
    }
    assertFalse(
        "Neither IP should lock user individually", lockManager.checkLock(TEST_USER_ID, TEST_IP));
  }

  // ---------------- Cleaning Logic ----------------

  @Test
  public void testCleanExpiredLocks() {
    for (int i = 0; i < failedLoginAttempts; i++) {
      lockManager.recordFailure(TEST_USER_ID, TEST_IP);
    }
    assertTrue(lockManager.checkLock(TEST_USER_ID, TEST_IP));

    try {
      Field field = LoginLockManager.class.getDeclaredField("userIpLocks");
      field.setAccessible(true);
      @SuppressWarnings("unchecked")
      ConcurrentMap<String, LoginLockManager.UserLockInfo> userIpLocks =
          (ConcurrentMap<String, LoginLockManager.UserLockInfo>) field.get(lockManager);

      LoginLockManager.UserLockInfo lockInfo = userIpLocks.get(TEST_USER_ID + "@" + TEST_IP);

      Field tsField = LoginLockManager.UserLockInfo.class.getDeclaredField("failureTimestamps");
      tsField.setAccessible(true);

      @SuppressWarnings("unchecked")
      Deque<Long> timestamps = (Deque<Long>) tsField.get(lockInfo);

      timestamps.clear();
      timestamps.add(System.currentTimeMillis() - (passwordLockTimeMinutes * 60 * 1000L) - 5000);

    } catch (Exception e) {
      fail("Failed to modify failure timestamps via reflection: " + e.getMessage());
    }

    lockManager.cleanExpiredLocks();
    assertFalse(
        "User should be unlocked after cleaning", lockManager.checkLock(TEST_USER_ID, TEST_IP));
  }

  @Test
  public void testNotCleanIfRecentFailureExists() {
    lockManager.recordFailure(TEST_USER_ID, TEST_IP);
    lockManager.cleanExpiredLocks();
    assertFalse(
        "Should still keep recent failure record", lockManager.checkLock(TEST_USER_ID, TEST_IP));
  }

  // ---------------- Sliding Window Logic ----------------

  @Test
  public void testLockWithinTimeWindow() throws InterruptedException {
    for (int i = 0; i < failedLoginAttempts; i++) {
      lockManager.recordFailure(TEST_USER_ID, TEST_IP);
      Thread.sleep(200); // small delay but still within window
    }
    assertTrue(
        "User should be locked within time window", lockManager.checkLock(TEST_USER_ID, TEST_IP));
  }

  @Test
  public void testFailuresOutsideWindowNotCounted() throws Exception {
    lockManager.recordFailure(TEST_USER_ID, TEST_IP);

    // hack timestamps to simulate old failure
    try {
      Field field = LoginLockManager.class.getDeclaredField("userIpLocks");
      field.setAccessible(true);
      @SuppressWarnings("unchecked")
      ConcurrentMap<String, LoginLockManager.UserLockInfo> userIpLocks =
          (ConcurrentMap<String, LoginLockManager.UserLockInfo>) field.get(lockManager);

      LoginLockManager.UserLockInfo lockInfo = userIpLocks.get(TEST_USER_ID + "@" + TEST_IP);

      Field tsField = LoginLockManager.UserLockInfo.class.getDeclaredField("failureTimestamps");
      tsField.setAccessible(true);

      @SuppressWarnings("unchecked")
      Deque<Long> timestamps = (Deque<Long>) tsField.get(lockInfo);

      timestamps.clear();
      timestamps.add(System.currentTimeMillis() - (passwordLockTimeMinutes * 60 * 1000L) - 5000);

    } catch (Exception e) {
      fail("Reflection modification failed: " + e.getMessage());
    }

    lockManager.recordFailure(TEST_USER_ID, TEST_IP);
    assertFalse(
        "Old failures should not count toward new lock",
        lockManager.checkLock(TEST_USER_ID, TEST_IP));
  }

  // ---------------- Configuration and Invalid Input ----------------
  @Test
  public void testNullIpHandling() {
    lockManager.recordFailure(TEST_USER_ID, null);
    assertFalse("Null IP should not cause exception", lockManager.checkLock(TEST_USER_ID, null));
  }

  @Test
  public void testNegativeUserId() {
    long negativeUserId = -123L;
    for (int i = 0; i < failedLoginAttempts; i++) {
      lockManager.recordFailure(negativeUserId, TEST_IP);
    }
    assertTrue(
        "Negative user ID should still lock", lockManager.checkLock(negativeUserId, TEST_IP));
  }

  // ---------------- Concurrency Stress Tests ----------------

  @Test
  public void testConcurrentFailuresCauseLock() throws InterruptedException {
    // Test configuration: 10 threads each making 2 failure attempts
    int threadCount = 10;
    int attemptsPerThread = 2;
    int totalAttempts = threadCount * attemptsPerThread; // Should exceed lock threshold

    // Task definition: each thread records multiple failures
    Runnable recordTask =
        () -> {
          for (int i = 0; i < attemptsPerThread; i++) {
            lockManager.recordFailure(TEST_USER_ID, TEST_IP);
          }
        };

    // Thread management: create and start all threads
    Thread[] threads = new Thread[threadCount];
    for (int i = 0; i < threadCount; i++) {
      threads[i] = new Thread(recordTask);
      threads[i].start();
    }

    // Synchronization: wait for all threads to complete
    for (Thread t : threads) {
      t.join();
    }

    // Verification: should be locked after concurrent attempts
    assertTrue(
        "System should be locked after " + totalAttempts + " concurrent failure attempts",
        lockManager.checkLock(TEST_USER_ID, TEST_IP));
  }

  @Test
  public void testConcurrentDifferentUsersIndependent() throws InterruptedException {
    // Test configuration: 500 test users with threshold attempts each
    int userCount = 500; // Large number to stress test isolation
    int attemptsPerUser = failedLoginAttempts; // Exactly reaching threshold

    // Task definition: simulate failures across multiple users
    Runnable userFailureTask =
        () -> {
          for (int u = 0; u < userCount; u++) {
            for (int i = 0; i < attemptsPerUser; i++) {
              lockManager.recordFailure(TEST_USER_ID + u, TEST_IP);
            }
          }
        };

    // Concurrent execution: two threads working on the same user set
    Thread t1 = new Thread(userFailureTask);
    Thread t2 = new Thread(userFailureTask);
    t1.start();
    t2.start();
    t1.join();
    t2.join();

    // Verification: each user should be locked independently
    for (int u = 0; u < userCount; u++) {
      assertTrue(
          "User " + u + " should be locked independently without affecting others",
          lockManager.checkLock(TEST_USER_ID + u, TEST_IP));
    }
  }

  @Test
  public void testConcurrentClearAndFailure() throws InterruptedException {
    // Setup: create locked state first
    for (int i = 0; i < failedLoginAttempts; i++) {
      lockManager.recordFailure(TEST_USER_ID, TEST_IP);
    }
    assertTrue(
        "Precondition: user should be locked before concurrency test",
        lockManager.checkLock(TEST_USER_ID, TEST_IP));

    // Task definitions:
    // Clear task: resets the failure count
    Runnable clearTask =
        () -> {
          lockManager.clearFailure(TEST_USER_ID, TEST_IP);
          // Artificial delay to increase race condition probability
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
          }
        };

    // Failure task: records additional failures
    Runnable failTask =
        () -> {
          lockManager.recordFailure(TEST_USER_ID, TEST_IP);
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
          }
        };

    // Concurrent execution: run clear and record operations simultaneously
    Thread clearThread = new Thread(clearTask);
    Thread failThread = new Thread(failTask);
    clearThread.start();
    failThread.start();
    clearThread.join();
    failThread.join();
    lockManager.checkLock(TEST_USER_ID, TEST_IP);
  }

  @Test
  public void testConcurrentLockStateConsistency() throws InterruptedException {
    // Test configuration: 5 checking threads, 1 recording thread
    int threadCount = 5;
    AtomicBoolean consistencyFlag = new AtomicBoolean(true);

    // Checking task: continuously verifies lock state validity
    Runnable checkTask =
        () -> {
          for (int i = 0; i < 100; i++) {
            boolean locked = lockManager.checkLock(TEST_USER_ID, TEST_IP);
            if (locked) {
              // When locked, verify the internal state matches
              try {
                Field counterField = LoginLockManager.class.getDeclaredField("userIpLocks");
                counterField.setAccessible(true);
                ConcurrentMap<?, ?> locks = (ConcurrentMap<?, ?>) counterField.get(lockManager);

                // Critical check: locked state should have non-empty counters
                if (locks.isEmpty()) {
                  consistencyFlag.set(false); // State inconsistency detected
                }
              } catch (Exception e) {
                consistencyFlag.set(false);
              }
            }
          }
        };

    // Recording task: pushes the system to locked state
    Runnable recordTask =
        () -> {
          for (int i = 0; i < failedLoginAttempts * 2; i++) {
            lockManager.recordFailure(TEST_USER_ID, TEST_IP);
          }
        };

    // Execution: start all checking threads and recording thread
    Thread[] checkThreads = new Thread[threadCount];
    for (int i = 0; i < threadCount; i++) {
      checkThreads[i] = new Thread(checkTask);
      checkThreads[i].start();
    }
    Thread recordThread = new Thread(recordTask);
    recordThread.start();

    // Synchronization: wait for all threads to complete
    for (Thread t : checkThreads) {
      t.join();
    }
    recordThread.join();

    // Final verification: no inconsistencies detected during test
    assertTrue("Lock state should remain valid during concurrent access", consistencyFlag.get());
  }

  @Test
  public void testConcurrentOperateLockInfo() throws InterruptedException, ExecutionException {
    int numThreads = 100;
    final int numAttempts = 100000;
    UserLockInfo userLockInfo = new UserLockInfo(numThreads * numAttempts);
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    List<Future<Void>> threads = new ArrayList<>(numThreads);
    for (int i = 0; i < numThreads; i++) {
      threads.add(
          executor.submit(
              () -> {
                for (int i1 = 0; i1 < numAttempts; i1++) {
                  userLockInfo.addFailureTime(i1);
                  if (i1 > 30) {
                    userLockInfo.removeOldFailures(i1 - 30);
                  }
                }
                return null;
              }));
    }
    for (Future<Void> thread : threads) {
      thread.get();
    }
  }
}
