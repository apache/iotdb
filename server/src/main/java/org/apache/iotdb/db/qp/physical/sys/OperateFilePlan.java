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
package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.io.File;
import java.util.Collections;
import java.util.List;

public class OperateFilePlan extends PhysicalPlan {

  private File file;
  private File targetDir;
  private boolean autoCreateSchema;
  private int sgLevel;
  private boolean verifyMetadata;

  public OperateFilePlan(File file, OperatorType operatorType) {
    super(false, operatorType);
    this.file = file;
  }

  /**
   * used for generate loading tsfile physical plan.
   *
   * @param file the loading file
   * @param operatorType the operator type
   * @param autoCreateSchema auto create schema if needed
   * @param sgLevel the level of sg
   * @param verifyMetadata metadata check if needed
   */
  public OperateFilePlan(
      File file,
      OperatorType operatorType,
      boolean autoCreateSchema,
      int sgLevel,
      boolean verifyMetadata) {
    super(false, operatorType);
    this.file = file;
    this.autoCreateSchema = autoCreateSchema;
    this.sgLevel = sgLevel;
    this.verifyMetadata = verifyMetadata;
  }

  public OperateFilePlan(File file, File targetDir, OperatorType operatorType) {
    super(false, operatorType);
    this.file = file;
    this.targetDir = targetDir;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }

  public File getFile() {
    return file;
  }

  public File getTargetDir() {
    return targetDir;
  }

  public boolean isAutoCreateSchema() {
    return autoCreateSchema;
  }

  public int getSgLevel() {
    return sgLevel;
  }

  public boolean getVerifyMetadata() {
    return verifyMetadata;
  }

  @Override
  public String toString() {
    return "OperateFilePlan{"
        + "file="
        + file
        + ", targetDir="
        + targetDir
        + ", autoCreateSchema="
        + autoCreateSchema
        + ", sgLevel="
        + sgLevel
        + ", verify="
        + verifyMetadata
        + ", operatorType="
        + getOperatorType()
        + '}';
  }
}
