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

package org.apache.iotdb.confignode.consensus.request;

import org.apache.iotdb.confignode.consensus.request.write.table.AddTableColumnPlan;

import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.AddWritableViewColumnPlan;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ConfigPhysicalPlanVisitorTest {

  @Test
  public void testNegativePlanTypeIdIsDispatchedToTimechoVisitor() {
    final boolean[] openSourceVisitorVisited = new boolean[] {false};
    final ConfigPhysicalPlanVisitor<String, Void> visitor =
        new ConfigPhysicalPlanVisitor<String, Void>() {
          @Override
          public String visitPlan(final ConfigPhysicalPlan plan, final Void context) {
            openSourceVisitorVisited[0] = true;
            return "open-source";
          }
        };

    try {
      visitor.process(new NegativePlan(), null);
      Assert.fail("Expected timecho plan to be dispatched to the placeholder visitor");
    } catch (final UnsupportedOperationException e) {
      Assert.assertFalse(openSourceVisitorVisited[0]);
      Assert.assertTrue(e.getMessage().contains("-1"));
    }
  }

  @Test
  public void testWritableViewPlanDelegatesToGenericTableHandlerByDefault() {
    final ConfigPhysicalPlanVisitor<String, Void> visitor =
        new ConfigPhysicalPlanVisitor<String, Void>() {
          @Override
          public String visitPlan(final ConfigPhysicalPlan plan, final Void context) {
            return "open-source";
          }

          @Override
          public String visitAddTableColumn(
              final AddTableColumnPlan addTableColumnPlan, final Void context) {
            return "generic-table";
          }
        };

    Assert.assertEquals("generic-table", visitor.process(new AddWritableViewColumnPlan(), null));
  }

  private static class NegativePlan extends ConfigPhysicalPlan {

    private NegativePlan() {
      super(ConfigPhysicalPlanType.TestOnly);
    }

    @Override
    public short getPlanTypeId() {
      return -1;
    }

    @Override
    protected void serializeImpl(final DataOutputStream stream) throws IOException {
      stream.writeShort(getPlanTypeId());
    }

    @Override
    protected void deserializeImpl(final ByteBuffer buffer) throws IOException {
      // Nothing to deserialize for the test-only placeholder plan.
    }
  }
}
