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

package org.apache.iotdb.commons.schema.table;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternUtil;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.rpc.TSStatusCode;

public class TreeViewSchema {
  public static final String ORIGINAL_NAME = "__original_name";
  public static final String TREE_PATH_PATTERN = "__tree_path_pattern";
  public static final String RESTRICT = "__restrict";

  public static boolean isTreeViewTable(final TsTable table) {
    return table.getPropValue(TREE_PATH_PATTERN).isPresent();
  }

  public static boolean isRestrict(final TsTable table) {
    return table.getPropValue(RESTRICT).isPresent();
  }

  public static TSStatus setPathPattern(final TsTable table, final PartialPath pathPattern) {
    final String[] nodes = pathPattern.getNodes();
    if (!PathPatternUtil.isMultiLevelMatchWildcard(nodes[nodes.length - 1])) {
      return new TSStatus(TSStatusCode.ILLEGAL_PATH.getStatusCode())
          .setMessage("The last node must be '**'");
    }
    for (int i = nodes.length - 2; i >= 0; --i) {
      if (PathPatternUtil.hasWildcard(nodes[i])) {
        return new TSStatus(TSStatusCode.ILLEGAL_PATH.getStatusCode())
            .setMessage("The wildCard is not permitted to set before the last node");
      }
    }
    table.addProp(TREE_PATH_PATTERN, pathPattern.toString());
    return StatusUtils.OK;
  }

  private TreeViewSchema() {
    // Private constructor
  }
}
