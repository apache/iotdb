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
package org.apache.iotdb.db.qp.other;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.tsfile.read.common.Path;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

/** test ast node parsing on authorization */
@RunWith(Parameterized.class)
public class TSPlanContextAuthorTest {

  private static Path[] emptyPaths = new Path[] {};
  private static Path[] testPaths = new Path[] {new Path("root.node1.a", "b")};

  private String inputSQL;
  private Path[] paths;

  public TSPlanContextAuthorTest(String inputSQL, Path[] paths) {
    this.inputSQL = inputSQL;
    this.paths = paths;
  }

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {"CREATE USER username1 'password1'", emptyPaths},
          {"DROP USER username", emptyPaths},
          {"CREATE ROLE rolename", emptyPaths},
          {"DROP ROLE rolename", emptyPaths},
          {
            "GRANT USER username PRIVILEGES SET_STORAGE_GROUP,INSERT_TIMESERIES ON root.node1.a.b",
            testPaths
          },
          {
            "REVOKE USER username PRIVILEGES SET_STORAGE_GROUP,INSERT_TIMESERIES ON root.node1.a.b",
            testPaths
          },
          {
            "GRANT ROLE rolename PRIVILEGES SET_STORAGE_GROUP,INSERT_TIMESERIES ON root.node1.a.b",
            testPaths
          },
          {
            "REVOKE ROLE rolename PRIVILEGES SET_STORAGE_GROUP,INSERT_TIMESERIES ON root.node1.a.b",
            testPaths
          },
          {"GRANT rolename TO username", emptyPaths},
          {"REVOKE rolename FROM username", emptyPaths}
        });
  }

  @Test
  public void testAnalyzeAuthor() throws QueryProcessException {
    Planner processor = new Planner();
    AuthorPlan author = (AuthorPlan) processor.parseSQLToPhysicalPlan(inputSQL);
    if (author == null) {
      fail();
    }
    assertArrayEquals(paths, author.getPaths().stream().map(PartialPath::toTSFilePath).toArray());
  }
}
