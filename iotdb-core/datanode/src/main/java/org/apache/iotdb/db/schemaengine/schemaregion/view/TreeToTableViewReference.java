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

package org.apache.iotdb.db.schemaengine.schemaregion.view;

import java.util.Objects;

/** A view identity and its matching metadata. Equality depends only on database and table names. */
public final class TreeToTableViewReference {

  public static final String NUM_OF_TAGS = "num_of_tags";

  private final String databaseName;

  private final String tableName;

  private final boolean restrict;

  private final int numOfTags;

  public TreeToTableViewReference(
      String databaseName, String tableName, boolean restrict, int numOfTags) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.restrict = restrict;
    this.numOfTags = numOfTags;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public boolean isRestrict() {
    return restrict;
  }

  public int getNumOfTags() {
    return numOfTags;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof TreeToTableViewReference)) {
      return false;
    }
    TreeToTableViewReference that = (TreeToTableViewReference) obj;
    return Objects.equals(databaseName, that.databaseName)
        && Objects.equals(tableName, that.tableName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(databaseName, tableName);
  }

  @Override
  public String toString() {
    return "TreeToTableViewReference{"
        + "databaseName='"
        + databaseName
        + '\''
        + ", tableName='"
        + tableName
        + '\''
        + ", restrict="
        + restrict
        + ", numOfTags="
        + numOfTags
        + '}';
  }
}
