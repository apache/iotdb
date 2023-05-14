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
package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.OperateFilePlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

import java.io.File;

/**
 * operator for loading tsfile including four property: file, the loading file. autoCreateSchema,
 * need auto create schema or not. sglevel, the level of sg. metadataCheck, need check for metadata
 * or not.
 */
public class LoadFilesOperator extends Operator {

  private File file;
  private boolean autoCreateSchema;
  private int sgLevel;
  private boolean verifyMetadata;

  public LoadFilesOperator(
      File file, boolean autoCreateSchema, int sgLevel, boolean verifyMetadata) {
    super(SQLConstant.TOK_LOAD_FILES);
    this.file = file;
    this.autoCreateSchema = autoCreateSchema;
    this.sgLevel = sgLevel;
    this.verifyMetadata = verifyMetadata;
    this.operatorType = OperatorType.LOAD_FILES;
  }

  public File getFile() {
    return file;
  }

  public boolean isAutoCreateSchema() {
    return autoCreateSchema;
  }

  public int getSgLevel() {
    return sgLevel;
  }

  public void setAutoCreateSchema(boolean autoCreateSchema) {
    this.autoCreateSchema = autoCreateSchema;
  }

  public void setSgLevel(int sgLevel) {
    this.sgLevel = sgLevel;
  }

  public void setVerifyMetadata(boolean verifyMetadata) {
    this.verifyMetadata = verifyMetadata;
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    return new OperateFilePlan(
        file, OperatorType.LOAD_FILES, autoCreateSchema, sgLevel, verifyMetadata);
  }
}
