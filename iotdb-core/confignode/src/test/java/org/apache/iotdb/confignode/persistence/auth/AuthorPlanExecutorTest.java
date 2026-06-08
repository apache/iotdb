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

package org.apache.iotdb.confignode.persistence.auth;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.authorizer.IAuthorizer;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorRelationalPlan;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorTreePlan;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AuthorPlanExecutorTest {

  @Test
  public void testAccountUnlockRequiresExistingUser() throws Exception {
    final IAuthorizer authorizer = mock(IAuthorizer.class);
    when(authorizer.getUser("missing")).thenReturn(null);

    final AuthorPlanExecutor executor = new AuthorPlanExecutor(authorizer);
    final TSStatus status =
        executor.executeAuthorNonQuery(
            new AuthorTreePlan(
                ConfigPhysicalPlanType.AccountUnlock,
                "missing",
                "",
                "",
                "",
                Collections.emptySet(),
                false,
                Collections.emptyList()));

    Assert.assertEquals(TSStatusCode.USER_NOT_EXIST.getStatusCode(), status.getCode());
  }

  @Test
  public void testRAccountUnlockRequiresExistingUser() throws Exception {
    final IAuthorizer authorizer = mock(IAuthorizer.class);
    when(authorizer.getUser("missing")).thenReturn(null);

    final AuthorPlanExecutor executor = new AuthorPlanExecutor(authorizer);
    final TSStatus status =
        executor.executeRelationalAuthorNonQuery(
            new AuthorRelationalPlan(
                ConfigPhysicalPlanType.RAccountUnlock,
                "missing",
                "",
                "",
                "",
                Collections.emptySet(),
                false,
                ""));

    Assert.assertEquals(TSStatusCode.USER_NOT_EXIST.getStatusCode(), status.getCode());
  }
}
