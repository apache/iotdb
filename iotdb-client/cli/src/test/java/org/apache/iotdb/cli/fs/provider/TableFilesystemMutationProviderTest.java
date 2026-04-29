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

package org.apache.iotdb.cli.fs.provider;

import org.apache.iotdb.cli.fs.path.FsPath;
import org.apache.iotdb.cli.fs.sql.SqlExecutor;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;

public class TableFilesystemMutationProviderTest {

  @Mock private SqlExecutor executor;

  private TableFilesystemMutationProvider provider;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    provider = new TableFilesystemMutationProvider(executor);
  }

  @Test
  public void mkdirDatabaseCreatesDatabase() throws SQLException {
    provider.mkdir(FsPath.absolute("/db1"));

    verify(executor).execute("CREATE DATABASE db1");
  }

  @Test
  public void mkdirRejectsRootAndTableLevel() throws SQLException {
    assertInvalidOperation(() -> provider.mkdir(FsPath.absolute("/")));
    assertInvalidOperation(() -> provider.mkdir(FsPath.absolute("/db1/table1")));
  }

  @Test
  public void removeTableDropsTable() throws SQLException {
    provider.remove(FsPath.absolute("/db1/table1"));

    verify(executor).execute("DROP TABLE db1.table1");
  }

  @Test
  public void removeRejectsRootDatabaseAndColumnLevel() throws SQLException {
    assertInvalidOperation(() -> provider.remove(FsPath.absolute("/")));
    assertInvalidOperation(() -> provider.remove(FsPath.absolute("/db1")));
    assertInvalidOperation(() -> provider.remove(FsPath.absolute("/db1/table1/s1")));
  }

  @Test
  public void moveTableRenamesTableInSameDatabase() throws SQLException {
    provider.move(FsPath.absolute("/db1/table1"), FsPath.absolute("/db1/table2"));

    verify(executor).execute("ALTER TABLE db1.table1 RENAME TO table2");
  }

  @Test
  public void moveRejectsUnsafeLevelsAndCrossDatabaseRename() throws SQLException {
    assertInvalidOperation(() -> provider.move(FsPath.absolute("/db1"), FsPath.absolute("/db2")));
    assertInvalidOperation(
        () -> provider.move(FsPath.absolute("/db1/table1/s1"), FsPath.absolute("/db1/table1/s2")));
    assertInvalidOperation(
        () -> provider.move(FsPath.absolute("/db1/table1"), FsPath.absolute("/db2/table1")));
  }

  private static void assertInvalidOperation(SqlOperation operation) throws SQLException {
    try {
      operation.run();
      fail();
    } catch (SQLException e) {
      assertEquals("Invalid filesystem write operation for this path", e.getMessage());
    }
  }

  private interface SqlOperation {
    void run() throws SQLException;
  }
}
