/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.mpp.common.object;

import org.junit.Assert;
import org.junit.Test;

public class MPPObjectPoolTest {

  @Test
  public void testResourceRelease() {
    MPPObjectPool objectPool = MPPObjectPool.getInstance();
    objectPool.clear();

    String queryId = "TEST";
    MPPObjectPool.QueryObjectPool queryObjectPool = objectPool.getQueryObjectPool(queryId);
    Assert.assertTrue(objectPool.hasQueryObjectPool(queryId));
    objectPool.clearQueryObjectPool(queryId);
    Assert.assertTrue(objectPool.hasQueryObjectPool(queryId));
    Assert.assertFalse(objectPool.isHasReleaseTask());

    System.gc();
    Assert.assertTrue(objectPool.hasQueryObjectPool(queryId));
    objectPool.clearQueryObjectPool(queryId);
    Assert.assertTrue(objectPool.hasQueryObjectPool(queryId));
    Assert.assertFalse(objectPool.isHasReleaseTask());

    queryObjectPool = null;
    System.gc();
    Assert.assertFalse(objectPool.hasQueryObjectPool(queryId));
    Assert.assertTrue(objectPool.hasQueryObjectPoolReference(queryId));
    Assert.assertFalse(objectPool.isHasReleaseTask());

    objectPool.clearQueryObjectPool(queryId);
    Assert.assertTrue(objectPool.isHasReleaseTask());
    while (objectPool.isHasReleaseTask()) ;
    Assert.assertFalse(objectPool.hasQueryObjectPool(queryId));
    Assert.assertFalse(objectPool.hasQueryObjectPoolReference(queryId));

    queryObjectPool = objectPool.getQueryObjectPool(queryId);
    Assert.assertTrue(objectPool.hasQueryObjectPool(queryId));

    objectPool.clear();
  }
}
