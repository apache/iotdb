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
package org.apache.iotdb.db.metadata.newnode.measurement;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

public class MeasurementInfo implements IMeasurementInfo {

  /** alias name of this measurement */
  protected String alias;
  /** tag/attribute's start offset in tag file */
  private long offset = -1;
  /** measurement's Schema for one timeseries represented by current leaf node */
  private final IMeasurementSchema schema;
  /** whether this measurement is pre deleted and considered in black list */
  private boolean preDeleted = false;

  public MeasurementInfo(IMeasurementSchema schema, String alias) {
    this.schema = schema;
    this.alias = alias;
  }

  @Override
  public IMeasurementSchema getSchema() {
    return schema;
  }

  @Override
  public TSDataType getDataType() {
    return schema.getType();
  }

  @Override
  public String getAlias() {
    return alias;
  }

  @Override
  public void setAlias(String alias) {
    this.alias = alias;
  }

  @Override
  public long getOffset() {
    return offset;
  }

  @Override
  public void setOffset(long offset) {
    this.offset = offset;
  }

  @Override
  public boolean isPreDeleted() {
    return preDeleted;
  }

  @Override
  public void setPreDeleted(boolean preDeleted) {
    this.preDeleted = preDeleted;
  }
}
