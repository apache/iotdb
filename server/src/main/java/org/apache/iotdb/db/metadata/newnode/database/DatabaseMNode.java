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
package org.apache.iotdb.db.metadata.newnode.database;

import org.apache.iotdb.db.metadata.mnode.BasicMNode;
import org.apache.iotdb.db.metadata.newnode.IMemMNode;

public class DatabaseMNode extends AbstractDatabaseMNode<IMemMNode, BasicMNode>
    implements IMemMNode {

  public DatabaseMNode(IMemMNode parent, String name) {
    super(name);
    this.basicMNode = new BasicMNode(parent, name);
  }

  // TODO: @yukun, remove this constructor
  public DatabaseMNode(IMemMNode parent, String name, long dataTTL) {
    this(parent, name);
    setDataTTL(dataTTL);
  }

  @Override
  public IMemMNode getAsMNode() {
    return this;
  }
}
