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

package org.apache.iotdb.commons.exception.table;

import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;

public class DropInvalidCategoryColumnException extends SemanticException {

  private final TsTableColumnCategory invalidCategory;

  public DropInvalidCategoryColumnException(final TsTableColumnCategory invalidCategory) {
    super(buildMessage(invalidCategory));
    this.invalidCategory = invalidCategory;
  }

  private static String buildMessage(final TsTableColumnCategory invalidCategory) {
    if (invalidCategory == TsTableColumnCategory.TAG
        || invalidCategory == TsTableColumnCategory.TIME) {
      return "Dropping tag or time column is not supported.";
    }
    return String.format(
        "Dropping %s column is not supported.", invalidCategory.name().toLowerCase());
  }

  public TsTableColumnCategory getInvalidCategory() {
    return invalidCategory;
  }
}
