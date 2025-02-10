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

package org.apache.iotdb.udf.api.relational.table.specification;

import java.util.Optional;

public class TableParameterSpecification extends ParameterSpecification {
  // set semantics or row semantics (default is set semantics)
  private final boolean rowSemantics;
  // prune when empty or keep when empty (default is keep when empty)
  private final boolean pruneWhenEmpty;
  private final boolean passThroughColumns;

  private TableParameterSpecification(
      String name, boolean rowSemantics, boolean pruneWhenEmpty, boolean passThroughColumns) {
    // table arguments are always required
    super(name, true, Optional.empty());
    this.rowSemantics = rowSemantics;
    this.pruneWhenEmpty = pruneWhenEmpty;
    this.passThroughColumns = passThroughColumns;
  }

  public boolean isRowSemantics() {
    return rowSemantics;
  }

  public boolean isPruneWhenEmpty() {
    return pruneWhenEmpty;
  }

  public boolean isPassThroughColumns() {
    return passThroughColumns;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private String name;
    private boolean rowSemantics;
    private boolean pruneWhenEmpty;
    private boolean passThroughColumns;

    private Builder() {}

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder setSemantics() {
      this.rowSemantics = false;
      return this;
    }

    public Builder rowSemantics() {
      this.rowSemantics = true;
      return this;
    }

    public Builder keepWhenEmpty() {
      this.pruneWhenEmpty = false;
      return this;
    }

    public Builder passThroughColumns() {
      this.passThroughColumns = true;
      return this;
    }

    public TableParameterSpecification build() {
      return new TableParameterSpecification(
          name, rowSemantics, pruneWhenEmpty, passThroughColumns);
    }
  }
}
