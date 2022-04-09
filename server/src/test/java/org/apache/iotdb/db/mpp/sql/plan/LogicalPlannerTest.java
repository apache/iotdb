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

package org.apache.iotdb.db.mpp.sql.plan;

import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.sql.analyze.Analysis;
import org.apache.iotdb.db.mpp.sql.parser.StatementGenerator;
import org.apache.iotdb.db.mpp.sql.planner.LogicalPlanner;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.write.AlterTimeSeriesNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.write.AuthorNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.mpp.sql.statement.Statement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.fail;

public class LogicalPlannerTest {

  LogicalPlanPrinter planPrinter = new LogicalPlanPrinter();

  @Before
  public void setUp() {}

  @Test
  @Ignore
  public void rawDataQueryTest() {
    PlanNode root =
        parseSQLToPlanNode(
            "SELECT s1,s2 FROM root.sg1.d1 WHERE time > 10 and s2 > 100 WITHOUT NULL ANY(s1) LIMIT 1 OFFSET 10");
    System.out.println(planPrinter.print(root));
    // TODO: replace all paths to full paths
    Assert.assertEquals(
        "[OffsetNode (7)]\n"
            + " │   RowOffset: 10\n"
            + " └─[LimitNode (6)]\n"
            + "    │   RowLimit: 1\n"
            + "    └─[FilterNullNode (5)]\n"
            + "       │   FilterNullPolicy: CONTAINS_NULL\n"
            + "       │   FilterNullColumnNames: [s1]\n"
            + "       └─[FilterNode (4)]\n"
            + "          │   QueryFilter: [and [time>10][s2>100]]\n"
            + "          └─[TimeJoinNode (3)]\n"
            + "             │   MergeOrder: TIMESTAMP_ASC\n"
            + "             │   FilterNullPolicy: null\n"
            + "             └─[SeriesScanNode (1)]\n"
            + "                │   SeriesPath: s1\n"
            + "                │   scanOrder: TIMESTAMP_ASC\n"
            + "               [SeriesScanNode (2)]\n"
            + "                │   SeriesPath: s2\n"
            + "                │   scanOrder: TIMESTAMP_ASC\n",
        planPrinter.print(root));
  }

  @Test
  @Ignore
  public void aggregationQueryTest() {
    PlanNode root =
        parseSQLToPlanNode(
            "SELECT sum(s1), avg(s2) FROM root.sg1.d1 WHERE time > 10 LIMIT 1 OFFSET 10");
    System.out.println(planPrinter.print(root));
    // TODO: replace all paths to full paths
    Assert.assertEquals(
        "[OffsetNode (6)]\n"
            + " │   RowOffset: 10\n"
            + " └─[LimitNode (5)]\n"
            + "    │   RowLimit: 1\n"
            + "    └─[FilterNode (4)]\n"
            + "       │   QueryFilter: [time>10]\n"
            + "       └─[TimeJoinNode (3)]\n"
            + "          │   MergeOrder: TIMESTAMP_ASC\n"
            + "          │   FilterNullPolicy: null\n"
            + "          └─[SeriesAggregateScanNode (2)]\n"
            + "             │   AggregateFunction: avg(s2)\n"
            + "            [SeriesAggregateScanNode (1)]\n"
            + "             │   AggregateFunction: sum(s1)\n",
        planPrinter.print(root));
  }

  @Test
  public void createTimeseriesPlanTest() {
    String sql =
        "CREATE TIMESERIES root.ln.wf01.wt01.status(状态) BOOLEAN ENCODING=PLAIN COMPRESSOR=SNAPPY TAGS(tag1=v1, tag2=v2) ATTRIBUTES(attr1=v1, attr2=v2)";
    try {
      CreateTimeSeriesNode createTimeSeriesNode = (CreateTimeSeriesNode) parseSQLToPlanNode(sql);
      Assert.assertNotNull(createTimeSeriesNode);
      Assert.assertEquals(
          new PartialPath("root.ln.wf01.wt01.status"), createTimeSeriesNode.getPath());
      Assert.assertEquals("状态", createTimeSeriesNode.getAlias());
      Assert.assertEquals(TSDataType.BOOLEAN, createTimeSeriesNode.getDataType());
      Assert.assertEquals(TSEncoding.PLAIN, createTimeSeriesNode.getEncoding());
      Assert.assertEquals(CompressionType.SNAPPY, createTimeSeriesNode.getCompressor());
      Assert.assertEquals(
          new HashMap<String, String>() {
            {
              put("tag1", "v1");
              put("tag2", "v2");
            }
          },
          createTimeSeriesNode.getTags());
      Assert.assertEquals(
          new HashMap<String, String>() {
            {
              put("attr1", "v1");
              put("attr2", "v2");
            }
          },
          createTimeSeriesNode.getAttributes());
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void createAlignedTimeseriesPlanTest() {
    String sql =
        "CREATE ALIGNED TIMESERIES root.ln.wf01.GPS(latitude(meter1) FLOAT encoding=PLAIN compressor=SNAPPY tags(tag1=t1) attributes(attr1=a1), longitude FLOAT encoding=PLAIN compressor=SNAPPY)";
    try {
      CreateAlignedTimeSeriesNode createAlignedTimeSeriesNode =
          (CreateAlignedTimeSeriesNode) parseSQLToPlanNode(sql);
      Assert.assertNotNull(createAlignedTimeSeriesNode);
      Assert.assertEquals(
          new PartialPath("root.ln.wf01.GPS"), createAlignedTimeSeriesNode.getDevicePath());
      Assert.assertEquals(
          new ArrayList<String>() {
            {
              add("meter1");
              add(null);
            }
          },
          createAlignedTimeSeriesNode.getAliasList());
      Assert.assertEquals(
          new ArrayList<TSDataType>() {
            {
              add(TSDataType.FLOAT);
              add(TSDataType.FLOAT);
            }
          },
          createAlignedTimeSeriesNode.getDataTypes());
      Assert.assertEquals(
          new ArrayList<TSEncoding>() {
            {
              add(TSEncoding.PLAIN);
              add(TSEncoding.PLAIN);
            }
          },
          createAlignedTimeSeriesNode.getEncodings());
      Assert.assertEquals(
          new ArrayList<CompressionType>() {
            {
              add(CompressionType.SNAPPY);
              add(CompressionType.SNAPPY);
            }
          },
          createAlignedTimeSeriesNode.getCompressors());
      Assert.assertEquals(
          new ArrayList<Map<String, String>>() {
            {
              add(
                  new HashMap<String, String>() {
                    {
                      put("attr1", "a1");
                    }
                  });
              add(null);
            }
          },
          createAlignedTimeSeriesNode.getAttributesList());
      Assert.assertEquals(
          new ArrayList<Map<String, String>>() {
            {
              add(
                  new HashMap<String, String>() {
                    {
                      put("tag1", "t1");
                    }
                  });
              add(null);
            }
          },
          createAlignedTimeSeriesNode.getTagsList());
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void alterTimeseriesPlanTest() {
    String sql = "ALTER timeseries root.turbine.d1.s1 RENAME tag1 TO newTag1";
    try {
      AlterTimeSeriesNode alterTimeSeriesNode = (AlterTimeSeriesNode) parseSQLToPlanNode(sql);
      Assert.assertNotNull(alterTimeSeriesNode);
      Assert.assertEquals(new PartialPath("root.turbine.d1.s1"), alterTimeSeriesNode.getPath());
      Assert.assertEquals(
          AlterTimeSeriesStatement.AlterType.RENAME, alterTimeSeriesNode.getAlterType());
      Assert.assertEquals(
          new HashMap<String, String>() {
            {
              put("tag1", "newTag1");
            }
          },
          alterTimeSeriesNode.getAlterMap());
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    }

    sql = "ALTER timeseries root.turbine.d1.s1 SET newTag1=newV1, attr1=newV1";
    try {
      AlterTimeSeriesNode alterTimeSeriesNode = (AlterTimeSeriesNode) parseSQLToPlanNode(sql);
      Assert.assertNotNull(alterTimeSeriesNode);
      Assert.assertEquals(new PartialPath("root.turbine.d1.s1"), alterTimeSeriesNode.getPath());
      Assert.assertEquals(
          AlterTimeSeriesStatement.AlterType.SET, alterTimeSeriesNode.getAlterType());
      Assert.assertEquals(
          new HashMap<String, String>() {
            {
              put("newTag1", "newV1");
              put("attr1", "newV1");
            }
          },
          alterTimeSeriesNode.getAlterMap());
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    }

    sql = "ALTER timeseries root.turbine.d1.s1 DROP tag1, tag2";
    try {
      AlterTimeSeriesNode alterTimeSeriesNode = (AlterTimeSeriesNode) parseSQLToPlanNode(sql);
      Assert.assertNotNull(alterTimeSeriesNode);
      Assert.assertEquals(new PartialPath("root.turbine.d1.s1"), alterTimeSeriesNode.getPath());
      Assert.assertEquals(
          AlterTimeSeriesStatement.AlterType.DROP, alterTimeSeriesNode.getAlterType());
      Assert.assertEquals(
          new HashMap<String, String>() {
            {
              put("tag1", null);
              put("tag2", null);
            }
          },
          alterTimeSeriesNode.getAlterMap());
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    }

    sql = "ALTER timeseries root.turbine.d1.s1 ADD TAGS tag3=v3, tag4=v4";
    try {
      AlterTimeSeriesNode alterTimeSeriesNode = (AlterTimeSeriesNode) parseSQLToPlanNode(sql);
      Assert.assertNotNull(alterTimeSeriesNode);
      Assert.assertEquals(new PartialPath("root.turbine.d1.s1"), alterTimeSeriesNode.getPath());
      Assert.assertEquals(
          AlterTimeSeriesStatement.AlterType.ADD_TAGS, alterTimeSeriesNode.getAlterType());
      Assert.assertEquals(
          new HashMap<String, String>() {
            {
              put("tag3", "v3");
              put("tag4", "v4");
            }
          },
          alterTimeSeriesNode.getAlterMap());
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    }

    sql = "ALTER timeseries root.turbine.d1.s1 ADD ATTRIBUTES attr3=v3, attr4=v4";
    try {
      AlterTimeSeriesNode alterTimeSeriesNode = (AlterTimeSeriesNode) parseSQLToPlanNode(sql);
      Assert.assertNotNull(alterTimeSeriesNode);
      Assert.assertEquals(new PartialPath("root.turbine.d1.s1"), alterTimeSeriesNode.getPath());
      Assert.assertEquals(
          AlterTimeSeriesStatement.AlterType.ADD_ATTRIBUTES, alterTimeSeriesNode.getAlterType());
      Assert.assertEquals(
          new HashMap<String, String>() {
            {
              put("attr3", "v3");
              put("attr4", "v4");
            }
          },
          alterTimeSeriesNode.getAlterMap());
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    }

    sql =
        "ALTER timeseries root.turbine.d1.s1 UPSERT ALIAS=newAlias TAGS(tag2=newV2, tag3=v3) ATTRIBUTES(attr3=v3, attr4=v4)";
    try {
      AlterTimeSeriesNode alterTimeSeriesNode = (AlterTimeSeriesNode) parseSQLToPlanNode(sql);
      Assert.assertNotNull(alterTimeSeriesNode);
      Assert.assertEquals(new PartialPath("root.turbine.d1.s1"), alterTimeSeriesNode.getPath());
      Assert.assertEquals(
          AlterTimeSeriesStatement.AlterType.UPSERT, alterTimeSeriesNode.getAlterType());
      Assert.assertEquals(
          new HashMap<String, String>() {
            {
              put("tag2", "newV2");
              put("tag3", "v3");
            }
          },
          alterTimeSeriesNode.getTagsMap());
      Assert.assertEquals(
          new HashMap<String, String>() {
            {
              put("attr3", "v3");
              put("attr4", "v4");
            }
          },
          alterTimeSeriesNode.getAttributesMap());
    } catch (IllegalPathException e) {
      e.printStackTrace();
      fail();
    }
  }

  private PlanNode parseSQLToPlanNode(String sql) {
    PlanNode planNode = null;
    try {
      Statement statement =
          StatementGenerator.createStatement(sql, ZonedDateTime.now().getOffset());
      MPPQueryContext context = new MPPQueryContext(new QueryId("test_query"));
      // TODO: do analyze after implementing ISchemaFetcher and IPartitionFetcher
      //      Analyzer analyzer = new Analyzer(context);
      //      Analysis analysis = analyzer.analyze(statement);
      Analysis analysis = new Analysis();
      analysis.setStatement(statement);
      LogicalPlanner planner = new LogicalPlanner(context, new ArrayList<>());
      planNode = planner.plan(analysis).getRootNode();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
    return planNode;
  }

  @Test
  public void authorTest() throws AuthException {

    String sql = null;
    AuthorNode authorNode = null;
    String[] privilegesList = {"DELETE_TIMESERIES"};

    // create user
    sql = "CREATE USER thulab 'passwd';";
    authorNode = (AuthorNode) parseSQLToPlanNode(sql);
    Assert.assertNotNull(authorNode);
    Assert.assertEquals(AuthorOperator.AuthorType.CREATE_USER, authorNode.getAuthorType());
    Assert.assertEquals("thulab", authorNode.getUserName());
    Assert.assertEquals("passwd", authorNode.getPassword());

    // create role
    sql = "CREATE ROLE admin;";
    authorNode = (AuthorNode) parseSQLToPlanNode(sql);
    Assert.assertNotNull(authorNode);
    Assert.assertEquals(AuthorOperator.AuthorType.CREATE_ROLE, authorNode.getAuthorType());
    Assert.assertEquals("admin", authorNode.getRoleName());

    // alter user
    sql = "ALTER USER tempuser SET PASSWORD 'newpwd';";
    authorNode = (AuthorNode) parseSQLToPlanNode(sql);
    Assert.assertNotNull(authorNode);
    Assert.assertEquals(AuthorOperator.AuthorType.UPDATE_USER, authorNode.getAuthorType());
    Assert.assertEquals("tempuser", authorNode.getUserName());
    Assert.assertEquals("newpwd", authorNode.getNewPassword());

    // grant user
    sql = "GRANT USER tempuser PRIVILEGES DELETE_TIMESERIES on root.ln;";
    authorNode = (AuthorNode) parseSQLToPlanNode(sql);
    Assert.assertNotNull(authorNode);
    Assert.assertEquals(AuthorOperator.AuthorType.GRANT_USER, authorNode.getAuthorType());
    Assert.assertEquals("tempuser", authorNode.getUserName());
    Assert.assertEquals(authorNode.strToPermissions(privilegesList), authorNode.getPermissions());
    Assert.assertEquals("root.ln", authorNode.getNodeName().getFullPath());

    // grant role
    sql = "GRANT ROLE temprole PRIVILEGES DELETE_TIMESERIES ON root.ln;";
    authorNode = (AuthorNode) parseSQLToPlanNode(sql);
    Assert.assertNotNull(authorNode);
    Assert.assertEquals(AuthorOperator.AuthorType.GRANT_ROLE, authorNode.getAuthorType());
    Assert.assertEquals("temprole", authorNode.getRoleName());
    Assert.assertEquals(authorNode.strToPermissions(privilegesList), authorNode.getPermissions());
    Assert.assertEquals("root.ln", authorNode.getNodeName().getFullPath());

    // grant role to user
    sql = "GRANT temprole TO tempuser;";
    authorNode = (AuthorNode) parseSQLToPlanNode(sql);
    Assert.assertNotNull(authorNode);
    Assert.assertEquals(AuthorOperator.AuthorType.GRANT_ROLE_TO_USER, authorNode.getAuthorType());
    Assert.assertEquals("temprole", authorNode.getRoleName());
    Assert.assertEquals("tempuser", authorNode.getUserName());

    // revoke user
    sql = "REVOKE USER tempuser PRIVILEGES DELETE_TIMESERIES on root.ln;";
    authorNode = (AuthorNode) parseSQLToPlanNode(sql);
    Assert.assertNotNull(authorNode);
    Assert.assertEquals(AuthorOperator.AuthorType.REVOKE_USER, authorNode.getAuthorType());
    Assert.assertEquals("tempuser", authorNode.getUserName());
    Assert.assertEquals(authorNode.strToPermissions(privilegesList), authorNode.getPermissions());
    Assert.assertEquals("root.ln", authorNode.getNodeName().getFullPath());

    // revoke role
    sql = "REVOKE ROLE temprole PRIVILEGES DELETE_TIMESERIES ON root.ln;";
    authorNode = (AuthorNode) parseSQLToPlanNode(sql);
    Assert.assertNotNull(authorNode);
    Assert.assertEquals(AuthorOperator.AuthorType.REVOKE_ROLE, authorNode.getAuthorType());
    Assert.assertEquals("temprole", authorNode.getRoleName());
    Assert.assertEquals(authorNode.strToPermissions(privilegesList), authorNode.getPermissions());
    Assert.assertEquals("root.ln", authorNode.getNodeName().getFullPath());

    // revoke role from user
    sql = "REVOKE temprole FROM tempuser;";
    authorNode = (AuthorNode) parseSQLToPlanNode(sql);
    Assert.assertNotNull(authorNode);
    Assert.assertEquals(
        AuthorOperator.AuthorType.REVOKE_ROLE_FROM_USER, authorNode.getAuthorType());
    Assert.assertEquals("temprole", authorNode.getRoleName());
    Assert.assertEquals("tempuser", authorNode.getUserName());

    // drop user
    sql = "DROP USER xiaoming;";
    authorNode = (AuthorNode) parseSQLToPlanNode(sql);
    Assert.assertNotNull(authorNode);
    Assert.assertEquals(AuthorOperator.AuthorType.DROP_USER, authorNode.getAuthorType());
    Assert.assertEquals("xiaoming", authorNode.getUserName());

    // drop role
    sql = "DROP ROLE admin;";
    authorNode = (AuthorNode) parseSQLToPlanNode(sql);
    Assert.assertNotNull(authorNode);
    Assert.assertEquals(AuthorOperator.AuthorType.DROP_ROLE, authorNode.getAuthorType());
    Assert.assertEquals("admin", authorNode.getRoleName());

    // list user
    sql = "LIST USER";
    authorNode = (AuthorNode) parseSQLToPlanNode(sql);
    Assert.assertNotNull(authorNode);
    Assert.assertEquals(AuthorOperator.AuthorType.LIST_USER, authorNode.getAuthorType());

    // list role
    sql = "LIST ROLE";
    authorNode = (AuthorNode) parseSQLToPlanNode(sql);
    Assert.assertNotNull(authorNode);
    Assert.assertEquals(AuthorOperator.AuthorType.LIST_ROLE, authorNode.getAuthorType());

    // list privileges user
    sql = "LIST PRIVILEGES USER sgcc_wirte_user ON root.sgcc;";
    authorNode = (AuthorNode) parseSQLToPlanNode(sql);
    Assert.assertNotNull(authorNode);
    Assert.assertEquals(AuthorOperator.AuthorType.LIST_USER_PRIVILEGE, authorNode.getAuthorType());

    // list privileges role
    sql = "LIST PRIVILEGES ROLE wirte_role ON root.sgcc;";
    authorNode = (AuthorNode) parseSQLToPlanNode(sql);
    Assert.assertNotNull(authorNode);
    Assert.assertEquals(AuthorOperator.AuthorType.LIST_ROLE_PRIVILEGE, authorNode.getAuthorType());

    // list user privileges
    sql = "LIST USER PRIVILEGES tempuser;";
    authorNode = (AuthorNode) parseSQLToPlanNode(sql);
    Assert.assertNotNull(authorNode);
    Assert.assertEquals(AuthorOperator.AuthorType.LIST_USER_PRIVILEGE, authorNode.getAuthorType());

    // list role privileges
    sql = "LIST ROLE PRIVILEGES actor;";
    authorNode = (AuthorNode) parseSQLToPlanNode(sql);
    Assert.assertNotNull(authorNode);
    Assert.assertEquals(AuthorOperator.AuthorType.LIST_ROLE_PRIVILEGE, authorNode.getAuthorType());

    // list all role of user
    sql = "LIST ALL ROLE OF USER tempuser;";
    authorNode = (AuthorNode) parseSQLToPlanNode(sql);
    Assert.assertNotNull(authorNode);
    Assert.assertEquals(AuthorOperator.AuthorType.LIST_USER_ROLES, authorNode.getAuthorType());

    // list all user of role
    sql = "LIST ALL USER OF ROLE roleuser;";
    authorNode = (AuthorNode) parseSQLToPlanNode(sql);
    Assert.assertNotNull(authorNode);
    Assert.assertEquals(AuthorOperator.AuthorType.LIST_ROLE_USERS, authorNode.getAuthorType());
  }
}
