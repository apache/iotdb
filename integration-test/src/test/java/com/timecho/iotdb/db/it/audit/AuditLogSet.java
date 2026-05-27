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

package com.timecho.iotdb.db.it.audit;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AuditLogSet {

  private static final Logger LOGGER = LoggerFactory.getLogger(AuditLogSet.class);

  private final int logCnt;
  private final HashMultiset<List<String>> auditLogSet;

  @SafeVarargs
  public AuditLogSet(List<String>... auditLogs) {
    logCnt = auditLogs.length;
    auditLogSet = Stream.of(auditLogs).collect(Collectors.toCollection(HashMultiset::create));
  }

  public void containAuditLog(ResultSet resultSet, Set<Integer> indexForContain, int columnCnt)
      throws SQLException {
    // Snapshot expected logs so repeated calls remain safe.
    HashMultiset<List<String>> remainingExpected = HashMultiset.create(auditLogSet);
    HashMultiset<List<String>> unmatchedActual = HashMultiset.create();

    // Read up to logCnt actual logs from the result set, preserving the iteration order.
    List<List<String>> actualLogs = new ArrayList<>();
    for (int curLog = 0; curLog < logCnt; curLog++) {
      if (!resultSet.next()) {
        break;
      }
      List<String> actualFields = new ArrayList<>(columnCnt);
      for (int i = 1; i <= columnCnt; i++) {
        actualFields.add(resultSet.getString(i + 1));
      }
      actualLogs.add(actualFields);
    }

    // Greedy-match each actual log against any still-unmatched expected log.
    for (List<String> actualFields : actualLogs) {
      List<String> matchedExpected = null;
      for (List<String> expectedFields : remainingExpected.elementSet()) {
        if (matches(actualFields, expectedFields, indexForContain, columnCnt)) {
          matchedExpected = expectedFields;
          break;
        }
      }
      if (matchedExpected != null) {
        remainingExpected.remove(matchedExpected);
      } else {
        unmatchedActual.add(actualFields);
      }
    }

    boolean countMismatch = actualLogs.size() != logCnt;
    boolean failed = !unmatchedActual.isEmpty() || !remainingExpected.isEmpty() || countMismatch;
    if (!failed) {
      LOGGER.info("All {} expected audit log(s) matched.", logCnt);
      return;
    }

    LOGGER.error("=========== Audit log comparison FAILED ===========");
    LOGGER.error(
        "Expected {} log(s), read {} actual log(s). Unmatched actual: {}, unmatched expected: {}.",
        logCnt,
        actualLogs.size(),
        unmatchedActual.size(),
        remainingExpected.size());
    if (!unmatchedActual.isEmpty()) {
      LOGGER.error("----- ACTUAL log(s) not present in expected -----");
      for (Multiset.Entry<List<String>> entry : unmatchedActual.entrySet()) {
        LOGGER.error("  [x{}] {}", entry.getCount(), entry.getElement());
      }
    }
    if (!remainingExpected.isEmpty()) {
      LOGGER.error("----- EXPECTED log(s) that never appeared -----");
      for (Multiset.Entry<List<String>> entry : remainingExpected.entrySet()) {
        LOGGER.error("  [x{}] {}", entry.getCount(), entry.getElement());
      }
    }
    LOGGER.error("===================================================");
    Assert.fail(
        String.format(
            "Audit log mismatch: %d unmatched actual, %d unmatched expected, read %d/%d log(s).",
            unmatchedActual.size(), remainingExpected.size(), actualLogs.size(), logCnt));
  }

  private boolean matches(
      List<String> actual, List<String> expected, Set<Integer> indexForContain, int columnCnt) {
    for (int i = 1; i <= columnCnt; i++) {
      String actualValue = actual.get(i - 1);
      String expectedValue = expected.get(i - 1);
      if (indexForContain.contains(i)) {
        if (!actualValue.contains(expectedValue)) {
          return false;
        }
      } else if (!expectedValue.equals(actualValue)) {
        return false;
      }
    }
    return true;
  }
}
