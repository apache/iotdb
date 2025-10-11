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

package org.apache.iotdb.db.it.audit;

import com.google.common.collect.HashMultiset;
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
    LOGGER.info("===========================================================");
    for (List<String> auditLog : auditLogSet) {
      LOGGER.info("Expected audit log: {}", auditLog);
    }
    for (int curLog = 0; curLog < logCnt; curLog++) {
      resultSet.next();
      List<String> actualFields = new ArrayList<>();
      for (int i = 1; i <= columnCnt; i++) {
        actualFields.add(resultSet.getString(i + 1));
      }
      LOGGER.info("Actual audit log: {}", actualFields);
      boolean match = false;
      for (List<String> expectedFields : auditLogSet) {
        match = true;
        for (int i = 1; i <= columnCnt; i++) {
          if (indexForContain.contains(i)) {
            if (resultSet.getString(i + 1).contains(expectedFields.get(i - 1))) {
              continue;
            } else {
              match = false;
              break;
            }
          }
          if (!expectedFields.get(i - 1).equals(resultSet.getString(i + 1))) {
            match = false;
            break;
          }
        }
        if (match) {
          auditLogSet.remove(expectedFields);
          break;
        }
      }
      Assert.assertTrue(match);
    }
    Assert.assertTrue(auditLogSet.isEmpty());
  }
}
