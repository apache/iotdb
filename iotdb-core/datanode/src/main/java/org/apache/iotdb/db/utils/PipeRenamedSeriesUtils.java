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

package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsOfOneDeviceStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;

public final class PipeRenamedSeriesUtils {

  private PipeRenamedSeriesUtils() {
    // Utility class
  }

  public static void markPipeInsertStatementAllowingInvalidSeries(
      final InsertBaseStatement statement) {
    if (statement == null) {
      return;
    }

    statement.setAllowInsertIntoInvalidSeries(true);
    if (statement instanceof InsertRowsStatement insertRowsStatement) {
      insertRowsStatement
          .getInsertRowStatementList()
          .forEach(PipeRenamedSeriesUtils::markPipeInsertRowStatement);
    } else if (statement instanceof InsertRowsOfOneDeviceStatement insertRowsOfOneDeviceStatement) {
      insertRowsOfOneDeviceStatement
          .getInsertRowStatementList()
          .forEach(PipeRenamedSeriesUtils::markPipeInsertRowStatement);
    } else if (statement instanceof InsertMultiTabletsStatement insertMultiTabletsStatement) {
      insertMultiTabletsStatement
          .getInsertTabletStatementList()
          .forEach(PipeRenamedSeriesUtils::markPipeInsertTabletStatement);
    }
  }

  private static void markPipeInsertRowStatement(final InsertRowStatement statement) {
    if (statement != null) {
      statement.setAllowInsertIntoInvalidSeries(true);
    }
  }

  private static void markPipeInsertTabletStatement(final InsertTabletStatement statement) {
    if (statement != null) {
      statement.setAllowInsertIntoInvalidSeries(true);
    }
  }
}
