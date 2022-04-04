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
package org.apache.iotdb.commons.partition;

import java.util.Objects;

public class SchemaRegionId implements Comparable<SchemaRegionId> {
  private int schemaRegionId;

  public SchemaRegionId(int schemaRegionId) {
    this.schemaRegionId = schemaRegionId;
  }

  public int getSchemaRegionId() {
    return schemaRegionId;
  }

  public void setSchemaRegionId(int schemaRegionId) {
    this.schemaRegionId = schemaRegionId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SchemaRegionId that = (SchemaRegionId) o;
    return schemaRegionId == that.schemaRegionId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(schemaRegionId);
  }

  @Override
  public String toString() {
    return String.format("SchemaRegion-%d", schemaRegionId);
  }

  @Override
  public int compareTo(SchemaRegionId o) {
    return this.getSchemaRegionId() - o.getSchemaRegionId();
  }
}
