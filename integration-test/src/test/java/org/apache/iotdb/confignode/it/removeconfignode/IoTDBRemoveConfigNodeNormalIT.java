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

package org.apache.iotdb.confignode.it.removeconfignode;

import org.apache.iotdb.confignode.it.removedatanode.SQLModel;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@Category({ClusterIT.class})
@RunWith(IoTDBTestRunner.class)
public class IoTDBRemoveConfigNodeNormalIT extends IoTDBRemoveConfigNodeITFramework {
  @Test
  public void test3C1DUseTreeSQL() throws Exception {
    testRemoveConfigNode(1, 1, 3, 1, 2, SQLModel.TREE_MODEL_SQL);
  }

  @Test
  public void test3C1DUseTableSQL() throws Exception {
    testRemoveConfigNode(1, 1, 3, 1, 2, SQLModel.TABLE_MODEL_SQL);
  }
}
