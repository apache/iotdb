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
import org.apache.iotdb.db.schemaengine.lease.MetadataLeaseTestUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.iotdb.db.schemaengine.lease.MetadataLeaseTestUtils.T_FENCE_MS;

public class ClusterAuthorityFetcherLeaseTest {

  private TestClock clock;
  private MetadataLeaseManager leaseManager;

  @Before
  public void setUp() {
    clock = new TestClock();
    leaseManager = MetadataLeaseTestUtils.newManager(clock::nowNanos);
  }

  @Test
  public void fencedLeaseDropsPermissionCache() {
    final ClusterAuthorityFetcher fetcher =
        new TestingClusterAuthorityFetcher(new BasicAuthorityCache(), leaseManager);
    final User user = new User("user_fenced", "password");
    fetcher.getAuthorCache().putUserCache(user.getName(), user);
    Assert.assertNotNull(fetcher.getAuthorCache().getUserCache(user.getName()));

    clock.addMillis(T_FENCE_MS + 1);
    fetcher.checkCacheAvailable();

    Assert.assertNull(
        "a fenced DataNode must drop its permission cache so a missed REVOKE cannot keep authorizing",
        fetcher.getAuthorCache().getUserCache(user.getName()));
  }

  @Test
  public void activeLeaseKeepsPermissionCache() {
    final ClusterAuthorityFetcher fetcher =
        new TestingClusterAuthorityFetcher(new BasicAuthorityCache(), leaseManager);
    final User user = new User("user_active", "password");
    fetcher.getAuthorCache().putUserCache(user.getName(), user);

    // An active lease (a ConfigNode heartbeat was just received) must not needlessly drop the
    // cache.
    clock.addMillis(1_000L);
    fetcher.checkCacheAvailable();

    Assert.assertNotNull(
        "an active lease must not needlessly drop the permission cache",
        fetcher.getAuthorCache().getUserCache(user.getName()));
  }

  private static class TestClock {
    private long nowNanos = 100_000_000_000L;

    private long nowNanos() {
      return nowNanos;
    }

    private void addMillis(final long millis) {
      nowNanos += millis * 1_000_000L;
    }
  }

  private static class TestingClusterAuthorityFetcher extends ClusterAuthorityFetcher {

    private final MetadataLeaseManager leaseManager;

    private TestingClusterAuthorityFetcher(
        final IAuthorCache authorCache, final MetadataLeaseManager leaseManager) {
      super(authorCache);
      this.leaseManager = leaseManager;
    }

    @Override
    boolean isMetadataLeaseFenced() {
      return MetadataLeaseTestUtils.isFenced(leaseManager);
    }
  }
}
