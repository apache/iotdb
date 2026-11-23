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

package org.apache.iotdb.db.pipe.source;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.leaf.TimeSeriesViewOperand;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.pipe.source.schemaregion.PipePlanTreePrivilegeParseVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.view.AlterLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.view.CreateLogicalViewNode;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class PipePlanTreePrivilegeParseVisitorTest {

  @Test
  public void testMissingSourceUserIsSkippedWhenConfigured() throws Exception {
    final PlanNode planNode = createTimeSeriesPlan();

    final Optional<PlanNode> result =
        new PipePlanTreePrivilegeParseVisitor(true).process(planNode, (String) null);

    Assert.assertFalse(result.isPresent());
  }

  @Test(expected = PipeException.class)
  public void testMissingSourceUserFailsWhenSkipIsDisabled() throws Exception {
    new PipePlanTreePrivilegeParseVisitor(false).process(createTimeSeriesPlan(), (String) null);
  }

  @Test
  public void testSuperUserCanTransferSchemaPlan() throws Exception {
    final PlanNode planNode = createTimeSeriesPlan();

    final Optional<PlanNode> result =
        new PipePlanTreePrivilegeParseVisitor(false).process(planNode, AuthorityChecker.SUPER_USER);

    Assert.assertSame(planNode, result.get());
  }

  @Test
  public void testDottedMeasurementRequiresExactSchemaPrivilege() throws Exception {
    final String username = "pipeDottedMeasurementSchemaUser";
    final User user = new User(username, "password");
    user.addPathPrivilege(
        new PartialPath("root.sg.d.s.**"), PrivilegeType.READ_SCHEMA.ordinal(), false);
    AuthorityChecker.getAuthorityFetcher().getAuthorCache().putUserCache(username, user);

    try {
      Assert.assertTrue(
          AuthorityChecker.checkFullPathListPermission(
                  username,
                  Collections.singletonList(new PartialPath("root.sg.d.s.one")),
                  PrivilegeType.READ_SCHEMA.ordinal())
              .isEmpty());

      final PlanNode planNode =
          new CreateAlignedTimeSeriesNode(
              new PlanNodeId("dotted-measurement"),
              new PartialPath("root.sg.d"),
              Collections.singletonList("s.one"),
              Collections.singletonList(TSDataType.INT32),
              Collections.singletonList(TSEncoding.PLAIN),
              Collections.singletonList(CompressionType.UNCOMPRESSED),
              null,
              null,
              null);

      Assert.assertFalse(
          new PipePlanTreePrivilegeParseVisitor(true).process(planNode, username).isPresent());
    } finally {
      AuthorityChecker.invalidateCache(username, null);
    }
  }

  @Test
  public void testLogicalViewSourcePathsRequireReadSchemaPrivilege() throws Exception {
    final String username = "pipeLogicalViewSourceUser";
    final User user = new User(username, "password");
    user.addPathPrivilege(
        new PartialPath("root.sg.target.**"), PrivilegeType.READ_SCHEMA.ordinal(), false);
    user.addPathPrivilege(
        new PartialPath("root.sg.target.**"), PrivilegeType.WRITE_SCHEMA.ordinal(), false);
    AuthorityChecker.getAuthorityFetcher().getAuthorCache().putUserCache(username, user);

    final Map<PartialPath, ViewExpression> viewMap = new HashMap<>();
    viewMap.put(
        new PartialPath("root.sg.target.view"),
        new TimeSeriesViewOperand("root.sg.secret.measurement"));
    final PlanNode createNode = new CreateLogicalViewNode(new PlanNodeId("create-view"), viewMap);
    final PlanNode alterNode = new AlterLogicalViewNode(new PlanNodeId("alter-view"), viewMap);

    try {
      final PartialPath targetPath = new PartialPath("root.sg.target.view");
      Assert.assertTrue(
          AuthorityChecker.checkFullPathListPermission(
                  username,
                  Collections.singletonList(targetPath),
                  PrivilegeType.READ_SCHEMA.ordinal())
              .isEmpty());
      Assert.assertTrue(
          AuthorityChecker.checkFullPathListPermission(
                  username,
                  Collections.singletonList(targetPath),
                  PrivilegeType.WRITE_SCHEMA.ordinal())
              .isEmpty());
      Assert.assertFalse(
          new PipePlanTreePrivilegeParseVisitor(true).process(createNode, username).isPresent());
      Assert.assertFalse(
          new PipePlanTreePrivilegeParseVisitor(true).process(alterNode, username).isPresent());
      Assert.assertNotEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          ((AlterLogicalViewNode) alterNode).checkPermissionBeforeProcess(username).getCode());
      try {
        new PipePlanTreePrivilegeParseVisitor(false).process(createNode, username);
        Assert.fail("Expected missing source READ_SCHEMA privilege to fail.");
      } catch (final PipeException expected) {
        Assert.assertNotNull(expected.getMessage());
      }
    } finally {
      AuthorityChecker.invalidateCache(username, null);
    }
  }

  private static CreateTimeSeriesNode createTimeSeriesPlan() throws Exception {
    return new CreateTimeSeriesNode(
        new PlanNodeId("schema"),
        new MeasurementPath("root.sg.d.s"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null,
        null,
        null,
        null);
  }
}
