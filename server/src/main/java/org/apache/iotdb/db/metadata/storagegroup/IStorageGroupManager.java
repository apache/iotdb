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
package org.apache.iotdb.db.metadata.storagegroup;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface IStorageGroupManager {

  void init() throws IOException;

  void clear() throws IOException;

  void setStorageGroup(PartialPath path) throws MetadataException;

  List<IMeasurementMNode> deleteStorageGroup(PartialPath path) throws MetadataException;

  SGMManager getSGMManager(PartialPath path) throws MetadataException;

  List<SGMManager> getInvolvedSGMManager(PartialPath pathPattern) throws MetadataException;

  List<SGMManager> getAllSGMManager();

  boolean isStorageGroup(PartialPath path);

  boolean checkStorageGroupByPath(PartialPath path);

  PartialPath getBelongedStorageGroup(PartialPath path) throws StorageGroupNotSetException;

  List<PartialPath> getBelongedStorageGroups(PartialPath pathPattern) throws MetadataException;

  List<PartialPath> getMatchedStorageGroups(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException;

  List<PartialPath> getAllStorageGroupPaths();

  Map<String, List<PartialPath>> groupPathByStorageGroup(PartialPath path) throws MetadataException;

  int getStorageGroupNum(PartialPath pathPattern, boolean isPrefixMatch) throws MetadataException;

  int getStorageGroupNum(PartialPath pathPattern) throws MetadataException;

  IStorageGroupMNode getStorageGroupNodeByStorageGroupPath(PartialPath path)
      throws MetadataException;

  IStorageGroupMNode getStorageGroupNodeByPath(PartialPath path) throws MetadataException;

  List<IStorageGroupMNode> getAllStorageGroupNodes();
}
