/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.metadata.plan.schemaregion.result;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.query.info.ISchemaInfo;

public abstract class ShowSchemaResult implements Comparable<ShowSchemaResult>, ISchemaInfo {

  protected String path;

  protected ShowSchemaResult(String path) {
    this.path = path;
  }

  protected ShowSchemaResult() {}

  public String getPath() {
    return path;
  }

  @Override
  public String getFullPath() {
    return path;
  }

  @Override
  public PartialPath getPartialPath() {
    try {
      return new PartialPath(path);
    } catch (IllegalPathException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int compareTo(ShowSchemaResult o) {
    return this.getPath().compareTo(o.getPath());
  }
}
