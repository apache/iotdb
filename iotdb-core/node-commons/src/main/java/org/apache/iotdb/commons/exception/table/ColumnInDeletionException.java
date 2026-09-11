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

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.i18n.CommonMessages;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.rpc.TSStatusCode;

public class ColumnInDeletionException extends MetadataException {

  public ColumnInDeletionException(
      final String database, final String tableName, final String columnName) {
    super(
        String.format(
            CommonMessages
                .EXCEPTION_COLUMN_ARG_IN_TABLE_ARG_ARG_IS_BEING_DELETED_PLEASE_WAIT_FOR_DELETION_TO_FINISH_OR_RETRY_DROPPING_THE_COLUMN_IF_IT_IS_STUCK_875DAFFE,
            columnName,
            PathUtils.unQualifyDatabaseName(database),
            tableName),
        TSStatusCode.SEMANTIC_ERROR.getStatusCode());
  }
}
