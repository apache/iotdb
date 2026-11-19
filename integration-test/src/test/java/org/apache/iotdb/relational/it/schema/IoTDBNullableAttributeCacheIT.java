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
package org.apache.iotdb.relational.it.schema;

import org.apache.iotdb.confignode.manager.lease.ClusterCachePropagator;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.relational.it.schema.fault.MetadataFaultController.Node;

import org.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category(TableClusterIT.class)
public class IoTDBNullableAttributeCacheIT extends TableMetadataPropagationTestBase {
  /** an unacked node keeps renewing and cannot be treated as fenced. */
  @Test
  public void testMissingInvalidationWithLiveLease() throws Exception {
    invalidationFailure(false);
  }

  /** Same topology and boundary, with successful execution followed by a lost transport ACK. */
  @Test
  public void testLostInvalidationAckWithLiveLease() throws Exception {
    invalidationFailure(true);
  }

  private void invalidationFailure(boolean loseAck) throws Exception {
    create(loseAck ? "metadata_lost_ack" : "metadata_lost_request", 2);
    DataNodeWrapper remote = remote();
    Node target = NODES.get(remote);
    warm(remote, 2);
    Map<String, String> lease = target.lease();
    String hook = loseAck ? ACK : INVALIDATE;
    target.arm(hook, "DROP", database);
    for (Node node : NODES.values()) {
      node.arm(DELETE, "OBSERVE", TABLE);
    }
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<SQLException> ddl =
        executor.submit(
            () -> {
              try {
                update("ALTER TABLE sensor DROP COLUMN changing");
                return null;
              } catch (SQLException e) {
                return e;
              } catch (Exception e) {
                throw new AssertionError("DDL failed to converge after restoring its RPC", e);
              }
            });
    try {
      awaitMatches(target, hook);
      try (Connection connection = connection(remote);
          Statement statement = connection.createStatement();
          ResultSet result =
              statement.executeQuery(
                  "SELECT kept, reading FROM " + database + ".sensor WHERE device='d0'")) {
        TestUtils.assertResultSetEqualUnordered(
            result, "kept,reading,", java.util.Collections.singletonList("keep-0,10,"));
      }
      long start = System.nanoTime();
      long fenceMs = Long.parseLong(target.lease().get("threshold"));
      long proceedMs = fenceMs + ClusterCachePropagator.DEFAULT_PROCEED_MARGIN_MS;
      long observationMs =
          proceedMs
              + ClusterCachePropagator.BROADCAST_RPC_TIMEOUT_MS
                  * ClusterCachePropagator.BROADCAST_RPC_RETRY
              + 500;
      // Cross the coordinator's proceed deadline, while every heartbeat is still delivered.
      // This is an invariant checked on each poll, not a sleep used to guess a DDL phase.
      Awaitility.await()
          .pollInterval(100, TimeUnit.MILLISECONDS)
          .atMost(observationMs + 5_000, TimeUnit.MILLISECONDS)
          .until(
              () -> {
                assertLease(target, lease);
                if (ddl.isDone()) {
                  assertInvalidationFailure(ddl.get());
                }
                for (Node node : NODES.values()) {
                  assertEquals(
                      "Region deletion crossed the invalidation barrier",
                      "0",
                      node.status(DELETE).get("matches"));
                }
                long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                return elapsed > proceedMs + 500 && (ddl.isDone() || elapsed > observationMs);
              });
      assertTrue(Long.parseLong(target.status(hook).get("dropped")) > 1);
      assertTrue(
          "The target must continue renewing its live metadata lease",
          Long.parseLong(target.lease().get("heartbeats"))
              > Long.parseLong(lease.get("heartbeats")));
      if (loseAck) {
        // The exit gate can only be reached after invalidation and release of the schema lock.
        assertTrue(target.count(INVALIDATE) > 0);
      }
      target.release(hook);
      SQLException failure = ddl.get(30, TimeUnit.SECONDS);
      if (failure != null) {
        assertInvalidationFailure(failure);
        // DropTableColumnProcedure does not roll back its pre-delete marker. Reissuing the same
        // drop is supported: preDeleteColumn adds that marker idempotently and completes the drop.
        update("ALTER TABLE sensor DROP COLUMN changing");
      }
      TestUtils.tableResultSetEqualTestUnordered(
          "SELECT device, kept, reading FROM sensor",
          new String[] {"device", "kept", "reading"},
          new String[] {"d0,keep-0,10,", "d1,keep-1,11,"},
          database);
      update("ALTER TABLE sensor ADD COLUMN changing STRING ATTRIBUTE");
      assertTwoDevices(null, null, true);
      update("UPDATE sensor SET changing='after-ddl' WHERE device='d0'");
      awaitTwoDevices("after-ddl", null, true);
      assertLease(target, lease);
      assertAttributeFilter(1, null);
    } finally {
      target.release(hook);
      finish(executor);
    }
  }

  /** an acknowledged old detail must not retire a newer clear container. */
  @Test
  public void testDegradedAttributeBacklogRecoversWithoutAnotherUpdate() throws Exception {
    create("metadata_attribute_degrade", 16);
    update(
        "CREATE TABLE retained (device STRING TAG, changing STRING ATTRIBUTE, reading INT32 FIELD)");
    update("INSERT INTO retained(time,device,changing,reading) VALUES(1,'keep','retained',99)");
    DataNodeWrapper remote = remote();
    DataNodeWrapper source =
        EnvFactory.getEnv().getDataNodeWrapperList().stream()
            .filter(wrapper -> wrapper != remote)
            .findFirst()
            .get();
    Node target = NODES.get(remote);
    Node sender = NODES.get(source);
    warm(remote, 16);
    Map<String, String> lease = target.lease();
    awaitEmpty(sender);
    sender.arm(OLD_COMMIT, "HOLD_FIRST", database, "changing=old-ack;");
    target.arm(CONTAINER, "OBSERVE", database);
    update("UPDATE sensor SET changing='old-ack' WHERE device='d0'");
    try {
      sender.awaitHeld(OLD_COMMIT);
      Map<String, String> acknowledged = sender.status(OLD_COMMIT);
      assertEquals(acknowledged.toString(), "detail", acknowledged.get("batchType"));
      assertTrue(
          acknowledged.toString(), acknowledged.get("attributes").contains("changing=old-ack;"));
      Map<String, String> baseline = sender.updaters(database);
      assertEquals(baseline.toString(), "1", baseline.get("found"));
      assertTrue(baseline.toString(), Integer.parseInt(baseline.get("detail")) > 0);
      long capacity = Long.parseLong(baseline.get("capacity"));
      assertTrue(
          "This fixture must use a bounded schema budget: " + baseline, capacity < 1_048_576);
      assertEquals("true", baseline.get("allowCreate"));
      long available = capacity - Long.parseLong(baseline.get("usage"));
      assertTrue(
          "The fixture needs room for persistent attributes: " + baseline, available > 32_768);
      // The store and its uncommitted detail both account for these different-device values.
      // Each consumes about 60% of the available budget; dropping the detail relieves pressure.
      int length = (int) (available * 3 / 5 / 16);
      String suffix = String.join("", java.util.Collections.nCopies(length, "x"));
      List<String> attributes = new ArrayList<>();
      for (int i = 0; i < 16; i++) {
        String attribute = "final-" + i + "-" + suffix;
        attributes.add(attribute);
        update("UPDATE sensor SET changing='" + attribute + "' WHERE device='d" + i + "'");
      }
      update("UPDATE sensor SET changing=NULL WHERE device='d0'");
      attributes.set(0, null);
      Map<String, String> degraded = sender.updaters(database);
      assertTrue(
          degraded.toString(),
          Long.parseLong(degraded.get("degrades")) > Long.parseLong(baseline.get("degrades")));
      assertTrue(degraded.toString(), Integer.parseInt(degraded.get("clear")) > 0);
      assertEquals(
          "The old acknowledged batch is still before commit",
          "0",
          sender.status(OLD_COMMIT).get("finished"));
      target.arm(UPDATE, "HOLD_FIRST", database);
      sender.release(OLD_COMMIT);
      target.awaitHeld(UPDATE);
      assertEquals("clear", target.status(UPDATE).get("batchType"));
      assertTrue(
          sender.updaters(database).toString(),
          Integer.parseInt(sender.updaters(database).get("clear")) > 0);
      assertLease(target, lease);
      // From here onwards there are no new UPDATEs until the backlog and cache have recovered.
      target.release(UPDATE);
      awaitFinished(target, UPDATE);
      Awaitility.await()
          .atMost(20, TimeUnit.SECONDS)
          .until(() -> Integer.parseInt(target.status(CONTAINER).get("clear")) > 0);
      awaitEmpty(sender);
      assertDevices(attributes, false);
      TestUtils.tableResultSetEqualTestUnordered(
          "SELECT device, changing, reading FROM retained",
          new String[] {"device", "changing", "reading"},
          new String[] {"keep,retained,99,"},
          database);
      assertLease(target, lease);
      assertEquals("true", sender.updaters(database).get("allowCreate"));
      update("UPDATE sensor SET changing='after-pressure' WHERE device='d1'");
      attributes.set(1, "after-pressure");
      Awaitility.await()
          .atMost(20, TimeUnit.SECONDS)
          .untilAsserted(() -> assertDevices(attributes, false));
      awaitEmpty(sender);
      update("INSERT INTO retained(time,device,changing,reading) VALUES(2,'new','modeled',100)");
      TestUtils.tableResultSetEqualTestUnordered(
          "SELECT device, changing, reading FROM retained",
          new String[] {"device", "changing", "reading"},
          new String[] {"keep,retained,99,", "new,modeled,100,"},
          database);
      assertAttributeFilter(0, null);
    } finally {
      sender.release(OLD_COMMIT);
      if (!target.status(UPDATE).isEmpty()) {
        target.release(UPDATE);
      }
    }
  }
}
