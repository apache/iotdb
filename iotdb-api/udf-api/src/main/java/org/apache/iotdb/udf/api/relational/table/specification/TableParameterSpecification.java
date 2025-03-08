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

/**
 * TableParameterSpecification are classified by two characteristics:
 *
 * <ul>
 *   <li>1. Input tables have either row semantics or set semantics, as follows:
 *       <ul>
 *         <li>Row semantics means that the the result of the PTF as decided on a row-by-row basis.
 *             As an extreme example, the DBMS could atomize the input table into individual rows,
 *             and send each single row to a different processor.
 *         <li>Set semantics means that the outcome of the function depends on how the data is
 *             partitioned. A partition shall not be split across processors, nor may a processor
 *             handle more than one partition.
 *       </ul>
 *   <li>2. Whether the input table supports pass-through columns or not. Pass-through columns isa
 *       mechanism enabling the PTF to copy every column of an input row into columns of an output
 *       row.
 * </ul>
 */
public class TableParameterSpecification extends ParameterSpecification {
  // set semantics or row semantics (default is set semantics)
  private final boolean rowSemantics;
  private final boolean passThroughColumns;

  private TableParameterSpecification(
      String name, boolean rowSemantics, boolean passThroughColumns) {
    // table arguments are always required
    super(name, true, Optional.empty());
    this.rowSemantics = rowSemantics;
    this.passThroughColumns = passThroughColumns;
  }

  public boolean isRowSemantics() {
    return rowSemantics;
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

    public Builder passThroughColumns() {
      this.passThroughColumns = true;
      return this;
    }

    public TableParameterSpecification build() {
      return new TableParameterSpecification(name, rowSemantics, passThroughColumns);
    }
  }
}
