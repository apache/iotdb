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

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.schema.node.IMNode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.schema.IMeasurementSchema;

/** This interface defines a MeasurementMNode's operation interfaces. */
public interface IMeasurementMNode<N extends IMNode<N>> extends IMNode<N> {

  IMeasurementSchema getSchema();

  void setSchema(IMeasurementSchema schema);

  TSDataType getDataType();

  String getAlias();

  void setAlias(String alias);

  long getOffset();

  void setOffset(long offset);

  boolean isPreDeleted();

  void setPreDeleted(boolean preDeleted);

  MeasurementPath getMeasurementPath();

  boolean isLogicalView();
}
