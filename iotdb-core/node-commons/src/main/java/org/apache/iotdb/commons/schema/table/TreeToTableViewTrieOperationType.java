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

import org.apache.iotdb.commons.i18n.SchemaMessages;

public enum TreeToTableViewTrieOperationType {
  /** Register the view, or replace the entry stored for the same view. */
  ADD((byte) 0),
  /** Unregister the view. */
  REMOVE((byte) 1),

  /** Replace the registered entry of the view with the one computed from the new table. */
  ALTER_TAG((byte) 2),

  ALTER_RENAME((byte) 3),

  ALTER_AUTO_SYNC((byte) 4);

  private final byte operationType;

  TreeToTableViewTrieOperationType(final byte operationType) {
    this.operationType = operationType;
  }

  public byte getOperationType() {
    return operationType;
  }

  public static TreeToTableViewTrieOperationType getType(final byte type) {
    switch (type) {
      case -1:
        return null;
      case 0:
        return ADD;
      case 1:
        return REMOVE;
      case 2:
        return ALTER_TAG;
      case 3:
        return ALTER_RENAME;
      case 4:
        return ALTER_AUTO_SYNC;
      default:
        throw new IllegalArgumentException(
            String.format(
                SchemaMessages.EXCEPTION_UNKNOWN_TREE_TO_TABLE_VIEW_OPERATION_TYPE_ARG_C13CAC3F,
                type));
    }
  }
}
