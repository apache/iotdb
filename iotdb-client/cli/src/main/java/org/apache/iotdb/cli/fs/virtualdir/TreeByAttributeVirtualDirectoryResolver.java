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

package org.apache.iotdb.cli.fs.virtualdir;

import org.apache.iotdb.cli.fs.provider.FilesystemSchemaProvider;
import org.apache.iotdb.cli.fs.sql.SqlExecutor;

public class TreeByAttributeVirtualDirectoryResolver
    extends TreeByMetadataVirtualDirectoryResolver {

  static final String NAME = "by-attribute";

  public TreeByAttributeVirtualDirectoryResolver(
      SqlExecutor executor, FilesystemSchemaProvider delegate) {
    super(
        NAME,
        "Attributes",
        "Browse tree timeseries grouped by attribute key and value",
        "attribute-key",
        "attribute-value",
        executor,
        delegate);
  }
}
