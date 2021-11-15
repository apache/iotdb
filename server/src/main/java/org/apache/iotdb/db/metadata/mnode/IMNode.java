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
package org.apache.iotdb.db.metadata.mnode;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.template.Template;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/** This interface defines a MNode's operation interfaces. */
public interface IMNode extends Serializable {

  String getName();

  void setName(String name);

  IMNode getParent();

  void setParent(IMNode parent);

  String getFullPath();

  void setFullPath(String fullPath);

  PartialPath getPartialPath();

  boolean hasChild(String name);

  IMNode getChild(String name);

  void addChild(String name, IMNode child);

  IMNode addChild(IMNode child);

  void deleteChild(String name);

  void replaceChild(String oldChildName, IMNode newChildNode);

  Map<String, IMNode> getChildren();

  void setChildren(Map<String, IMNode> children);

  boolean isUseTemplate();

  Template getUpperTemplate();

  Template getSchemaTemplate();

  void setSchemaTemplate(Template schemaTemplate);

  // EmptyInternal means there's no child or template under this node
  // and this node is not the root nor a storageGroup nor a measurement.
  boolean isEmptyInternal();

  boolean isStorageGroup();

  boolean isEntity();

  boolean isMeasurement();

  IStorageGroupMNode getAsStorageGroupMNode();

  IEntityMNode getAsEntityMNode();

  IMeasurementMNode getAsMeasurementMNode();

  void serializeTo(MLogWriter logWriter) throws IOException;
}
