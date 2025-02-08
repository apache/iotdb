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
package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.info;

import org.apache.iotdb.commons.schema.node.IMNode;
import org.apache.iotdb.commons.schema.node.info.IDatabaseDeviceInfo;
import org.apache.iotdb.commons.schema.node.role.IDatabaseMNode;

public class DatabaseDeviceInfo<N extends IMNode<N>> extends TreeDeviceInfo<N>
    implements IDatabaseDeviceInfo<N> {
  /**
   * when the data file in a database is older than dataTTL, it is considered invalid and will be
   * eventually deleted. TODO: database ttl is useless here, which can be removed.
   */
  private long dataTTL = Long.MAX_VALUE;

  @Override
  public void moveDataToNewMNode(IDatabaseMNode<N> newMNode) {
    // Do nothing
  }

  /**
   * The memory occupied by an DatabaseDeviceInfo based occupation
   *
   * <ol>
   *   <li>long dataTTL, 8B
   * </ol>
   */
  @Override
  public int estimateSize() {
    return super.estimateSize() + 8;
  }
}
