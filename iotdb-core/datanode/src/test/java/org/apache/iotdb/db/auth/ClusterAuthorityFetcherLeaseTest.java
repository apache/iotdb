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

package org.apache.iotdb.db.auth;

import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.db.schemaengine.lease.MetadataLeaseManager;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * When the DataNode metadata lease has expired (no ConfigNode heartbeat within {@code T_fence}),
 * the permission cache must not be trusted: a partitioned DataNode could have missed a REVOKE
 * broadcast and would otherwise keep authorizing a privilege that was already revoked. {@link
 * ClusterAuthorityFetcher#checkCacheAvailable()} therefore drops the cache while fenced, forcing a
 * re-fetch from the ConfigNode (which fails closed while the DataNode is partitioned).
 *
 * <p>This closes a window the pre-existing {@code refreshToken()} timeout did not: that timeout
 * only marks the cache stale when a heartbeat finally arrives after a long gap, so during an
 * <em>ongoing</em> partition (no heartbeat at all) the stale cache kept being served. {@code
 * isFenced()} is evaluated on the DataNode's own clock and needs no heartbeat to fire.
 */
public class ClusterAuthorityFetcherLeaseTest {

  @After
  public void tearDown() {
    // Restore the process-wide lease singleton so other tests in this JVM are unaffected.
    MetadataLeaseManager.getInstance().recordConfigNodeHeartbeat();
  }

  @Test
  public void fencedLeaseDropsPermissionCache() {
    final ClusterAuthorityFetcher fetcher = new ClusterAuthorityFetcher(new BasicAuthorityCache());
    final User user = new User("user_fenced", "password");
    fetcher.getAuthorCache().putUserCache(user.getName(), user);
    Assert.assertNotNull(fetcher.getAuthorCache().getUserCache(user.getName()));

    MetadataLeaseManager.getInstance().expireLeaseForTest();
    fetcher.checkCacheAvailable();

    Assert.assertNull(
        "a fenced DataNode must drop its permission cache so a missed REVOKE cannot keep authorizing",
        fetcher.getAuthorCache().getUserCache(user.getName()));
  }

  @Test
  public void activeLeaseKeepsPermissionCache() {
    final ClusterAuthorityFetcher fetcher = new ClusterAuthorityFetcher(new BasicAuthorityCache());
    final User user = new User("user_active", "password");
    fetcher.getAuthorCache().putUserCache(user.getName(), user);

    // An active lease (a ConfigNode heartbeat was just received) must not needlessly drop the
    // cache.
    MetadataLeaseManager.getInstance().recordConfigNodeHeartbeat();
    fetcher.checkCacheAvailable();

    Assert.assertNotNull(
        "an active lease must not needlessly drop the permission cache",
        fetcher.getAuthorCache().getUserCache(user.getName()));
  }
}
