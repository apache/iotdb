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
package org.apache.iotdb.confignode.manager;

import org.junit.Assert;
import org.junit.Test;

public class ClusterSchemaManagerTest {

  @Test
  public void testCalcMaxRegionGroupNum() {

    // The maxRegionGroupNum should be great or equal to the leastRegionGroupNum
    Assert.assertEquals(100, ClusterSchemaManager.calcMaxRegionGroupNum(100, 1.0, 3, 1, 3, 0));

    // The maxRegionGroupNum should be great or equal to the allocatedRegionGroupCount
    Assert.assertEquals(100, ClusterSchemaManager.calcMaxRegionGroupNum(3, 1.0, 6, 2, 3, 100));

    // (resourceWeight * resource) / (createdStorageGroupNum * replicationFactor)
    Assert.assertEquals(20, ClusterSchemaManager.calcMaxRegionGroupNum(3, 1.0, 120, 2, 3, 5));
  }
}
