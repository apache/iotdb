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

package org.apache.iotdb.commons.schema.node.role;

import org.apache.iotdb.commons.schema.node.IMNode;
import org.apache.iotdb.commons.schema.node.common.DeviceMNodeWrapper;
import org.apache.iotdb.commons.schema.node.info.IDeviceInfo;

public interface IInternalMNode<N extends IMNode<N>> extends IMNode<N> {

  IDeviceInfo<N> getDeviceInfo();

  void setDeviceInfo(IDeviceInfo<N> deviceInfo);

  @Override
  default boolean isDevice() {
    return getDeviceInfo() != null;
  }

  @Override
  default boolean isMeasurement() {
    return false;
  }

  @Override
  default IInternalMNode<N> getAsInternalMNode() {
    return this;
  }

  @Override
  default IDeviceMNode<N> getAsDeviceMNode() {
    if (isDevice()) {
      return new DeviceMNodeWrapper<>(this);
    } else {
      throw new UnsupportedOperationException("Wrong node type");
    }
  }
}
