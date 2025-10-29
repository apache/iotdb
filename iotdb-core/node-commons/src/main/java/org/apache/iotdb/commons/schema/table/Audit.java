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

import org.apache.iotdb.commons.path.PartialPath;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.schema.SchemaConstant.ROOT;

public class Audit {
  public static final String TABLE_MODEL_AUDIT_DATABASE = "__audit";
  public static final String TREE_MODEL_AUDIT_DATABASE =
      String.format("%s.%s", ROOT, TABLE_MODEL_AUDIT_DATABASE);
  public static final PartialPath TREE_MODEL_AUDIT_DATABASE_PATH =
      new PartialPath(new String[] {"root", TABLE_MODEL_AUDIT_DATABASE});
  public static final PartialPath TREE_MODEL_AUDIT_DATABASE_PATH_PATTERN =
      new PartialPath(new String[] {"root", TABLE_MODEL_AUDIT_DATABASE, MULTI_LEVEL_PATH_WILDCARD});

  private Audit() {}

  /**
   * @param prefixPath without any * or **
   */
  public static boolean includeByAuditTreeDB(PartialPath prefixPath) {
    String[] nodes = prefixPath.getNodes();
    return nodes.length >= 2 && TABLE_MODEL_AUDIT_DATABASE.equalsIgnoreCase(nodes[1]);
  }
}
