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

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.template.Template;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

public interface IMNode extends Serializable {
  boolean hasChild(String name);

  void addChild(String name, IMNode child);

  IMNode addChild(IMNode child);

  void deleteChild(String name);

  void deleteAliasChild(String alias);

  Template getDeviceTemplate();

  void setDeviceTemplate(Template deviceTemplate);

  IMNode getChild(String name);

  IMNode getChildOfAlignedTimeseries(String name) throws MetadataException;

  int getMeasurementMNodeCount();

  boolean addAlias(String alias, IMNode child);

  String getFullPath();

  PartialPath getPartialPath();

  IMNode getParent();

  void setParent(IMNode parent);

  Map<String, IMNode> getChildren();

  Map<String, IMNode> getAliasChildren();

  void setChildren(Map<String, IMNode> children);

  void setAliasChildren(Map<String, IMNode> aliasChildren);

  String getName();

  void setName(String name);

  void serializeTo(MLogWriter logWriter) throws IOException;

  void replaceChild(String measurement, IMNode newChildNode);

  void setFullPath(String fullPath);

  Template getUpperTemplate();

  boolean isUseTemplate();

  void setUseTemplate(boolean useTemplate);
}
