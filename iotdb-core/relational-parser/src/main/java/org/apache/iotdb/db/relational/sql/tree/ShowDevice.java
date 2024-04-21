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

package org.apache.iotdb.db.relational.sql.tree;

import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

public class ShowDevice extends Statement {

  private Expression whereClause;

  public ShowDevice(@Nullable NodeLocation location) {
    super(location);
  }

  public void setWhereClause(Expression whereClause) {
    this.whereClause = whereClause;
  }

  public Expression getWhereClause() {
    return whereClause;
  }

  @Override
  public List<? extends Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return getClass().hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ShowDevice)) return false;
    ShowDevice that = (ShowDevice) o;
    return Objects.equals(whereClause, that.whereClause);
  }

  @Override
  public String toString() {
    return "ShowDevice{" + "whereClause=" + whereClause + '}';
  }
}
