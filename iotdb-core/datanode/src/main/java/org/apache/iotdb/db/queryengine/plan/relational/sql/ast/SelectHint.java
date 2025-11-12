/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import org.apache.iotdb.db.queryengine.plan.relational.utils.hint.Hint;

import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SelectHint extends Node {
  Map<String, Hint> hintMap;

  public SelectHint() {
    super(null);
    this.hintMap = new HashMap<>();
  }

  public SelectHint(Map<String, Hint> hintMap) {
    super(null);
    this.hintMap = hintMap;
  }

  public Map<String, Hint> getHintMap() {
    return hintMap;
  }

  @Override
  public List<? extends Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return Objects.hash(hintMap);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    SelectHint other = (SelectHint) obj;
    return Objects.equals(this.hintMap, other.hintMap);
  }

  @Override
  public String toString() {
    if (hintMap == null || hintMap.isEmpty()) {
      return "";
    }

    StringBuilder sb = new StringBuilder();
    sb.append("/*+ ");

    boolean first = true;
    for (Map.Entry<String, Hint> entry : hintMap.entrySet()) {
      if (!first) {
        sb.append(" ");
      }
      sb.append(entry.getValue().toString());
      first = false;
    }

    sb.append(" */");
    return sb.toString();
  }
}
