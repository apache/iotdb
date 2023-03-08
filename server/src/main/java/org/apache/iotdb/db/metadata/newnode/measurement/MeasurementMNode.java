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
package org.apache.iotdb.db.metadata.newnode.measurement;

import org.apache.iotdb.db.metadata.mnode.BasicMNode;
import org.apache.iotdb.db.metadata.mnode.container.IMNodeContainer;
import org.apache.iotdb.db.metadata.mnode.container.MemMNodeContainer;
import org.apache.iotdb.db.metadata.newnode.IMemMNode;
import org.apache.iotdb.db.metadata.newnode.device.IDeviceMNode;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

public class MeasurementMNode extends AbstractMeasurementMNode<IMemMNode, BasicMNode>
    implements IMemMNode {

  public MeasurementMNode(
      IDeviceMNode<IMemMNode> parent, String name, IMeasurementSchema schema, String alias) {
    super(schema, alias);
    this.basicMNode = new BasicMNode(parent == null ? null : parent.getAsMNode(), name);
  }

  @Override
  public IMemMNode getAsMNode() {
    return this;
  }

  @Override
  public IMNodeContainer<IMemMNode> getChildren() {
    return MemMNodeContainer.emptyMNodeContainer();
  }
}
