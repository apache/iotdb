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

package org.apache.iotdb.db.schemaengine.table;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.db.schemaengine.schemaregion.view.TreeToTableViewReference;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface ITreeToTableViewCache {

  void init(Map<String, List<TsTable>> initialTables);

  void clear();

  /**
   * Registers the view at {@code path}, the fixed prefix of its tree path pattern. {@code
   * isRestrict} and {@code numOfTags} are the two attributes a device path is matched with, see
   * {@link TreeToTableViewReference}.
   */
  boolean addView(
      String database, String tableName, boolean isRestrict, int numOfTags, PartialPath path)
      throws IllegalPathException;

  boolean removeView(String database, String tableName);

  /**
   * Removes every view of the database, eg. when the database is dropped. The views are taken from
   * the index itself, so the ones the table cache does not hold any more are removed as well.
   */
  void removeViews(String database);

  boolean alterViewName(String database, String newName, String oldName);

  /**
   * Replaces the fields named in {@code properties} on the registered view. Only the restrict flag
   * and the tag count can be replaced today, and a field whose key is absent keeps its value.
   */
  boolean alterViewProperties(String database, String tableName, Map<String, String> properties);

  @Nullable
  PartialPath getViewPath(String database, String tableName);

  Set<TreeToTableViewReference> getTreeToTableViews(PartialPath path);

  boolean hasView();
}
