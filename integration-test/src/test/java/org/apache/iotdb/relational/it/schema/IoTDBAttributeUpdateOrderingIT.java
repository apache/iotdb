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

import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.relational.it.schema.fault.MetadataFaultController.Node;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category(TableClusterIT.class)
public class IoTDBAttributeUpdateOrderingIT extends TableMetadataPropagationTestBase {
  /** a delayed batch belongs to the dropped column's earlier lifetime. */
  @Test
  public void testOldAttributeBatchAfterDropAndReadd() throws Exception {
    create("metadata_attribute_lifetime", 2);
    DataNodeWrapper remote = remote();
    Node target = NODES.get(remote);
    warm(remote, 2);
    Map<String, String> lease = target.lease();
    target.arm(UPDATE, "HOLD_FIRST", database, "changing=late-old;");
    update("UPDATE sensor SET changing='late-old' WHERE device='d0'");
    try {
      target.awaitHeld(UPDATE);
      Map<String, String> delayed = target.status(UPDATE);
      assertEquals("detail", delayed.get("batchType"));
      assertTrue(delayed.toString(), delayed.get("version").matches("\\d+=\\d+"));
      assertTrue(delayed.toString(), delayed.get("attributes").contains("changing=late-old"));
      update("ALTER TABLE sensor DROP COLUMN changing");
      update("ALTER TABLE sensor ADD COLUMN changing STRING ATTRIBUTE");
      assertTwoDevices(null, null, true);
      update("UPDATE sensor SET changing='new-lifetime' WHERE device='d0'");
      awaitTwoDevices("new-lifetime", null, true);
      assertEquals("0", target.status(UPDATE).get("finished"));
      assertLease(target, lease);
      target.release(UPDATE);
      awaitFinished(target, UPDATE);
      try {
        assertTwoDevices("new-lifetime", null, true);
      } catch (AssertionError e) {
        throw new AssertionError(
            "Completed old batch "
                + delayed.get("version")
                + " ("
                + delayed.get("attributes")
                + ") changed confirmed current data at "
                + remote.getIpAndPortString(),
            e);
      }
      update("UPDATE sensor SET changing='newer-lifetime' WHERE device='d0'");
      awaitTwoDevices("newer-lifetime", null, true);
      assertLease(target, lease);
    } finally {
      target.release(UPDATE);
    }
  }
}
