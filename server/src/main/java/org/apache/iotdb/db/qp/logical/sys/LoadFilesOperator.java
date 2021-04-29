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

import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.RootOperator;

import java.io.File;

public class LoadFilesOperator extends RootOperator {

  private File file;
  private boolean autoCreateSchema;
  private int sgLevel;

  public LoadFilesOperator(File file, boolean autoCreateSchema, int sgLevel) {
    super(SQLConstant.TOK_LOAD_FILES);
    this.file = file;
    this.autoCreateSchema = autoCreateSchema;
    this.sgLevel = sgLevel;
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
}
