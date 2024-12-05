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

import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.db.exception.sql.SemanticException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class InformationSchema {
  private static final String INFORMATION_DATABASE = "information_schema";
  private static final Map<String, TsTable> schemaTables = Collections.unmodifiableMap(new HashMap<>());

  public static void checkDBNameInWrite(final String dbName) {
    if (dbName.equals(INFORMATION_DATABASE)) {
      throw new SemanticException("The database 'information_schema' can only be queried");
    }
  }

  private InformationSchema() {
    // Util class
  }
}
