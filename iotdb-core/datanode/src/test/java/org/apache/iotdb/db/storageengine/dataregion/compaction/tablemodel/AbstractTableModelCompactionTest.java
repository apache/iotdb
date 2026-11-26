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

package org.apache.iotdb.db.storageengine.dataregion.compaction.tablemodel;

import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.utils.constant.TestConstant;

import java.io.File;

/**
 * The base class of the compaction tests whose source files contain table model data.
 *
 * <p>The data model of the compaction input is resolved from the database directory of the source
 * files, and a table model database name must not start with {@code root.}, therefore these tests
 * use a table model database instead of the tree model database used by the default fixture.
 */
public abstract class AbstractTableModelCompactionTest extends AbstractCompactionTest {

  /** A table model database name, which must not start with "root.". */
  public static final String TABLE_MODEL_TEST_SG = "testsg";

  public static final File TABLE_MODEL_SEQ_STORAGE_GROUP_DIR =
      new File(
          TestConstant.BASE_OUTPUT_PATH
              + "data"
              + File.separator
              + "sequence"
              + File.separator
              + TABLE_MODEL_TEST_SG);
  public static final File TABLE_MODEL_UNSEQ_STORAGE_GROUP_DIR =
      new File(
          TestConstant.BASE_OUTPUT_PATH
              + "data"
              + File.separator
              + "unsequence"
              + File.separator
              + TABLE_MODEL_TEST_SG);
  public static final File TABLE_MODEL_SEQ_DIRS =
      new File(TABLE_MODEL_SEQ_STORAGE_GROUP_DIR, "0" + File.separator + "0");
  public static final File TABLE_MODEL_UNSEQ_DIRS =
      new File(TABLE_MODEL_UNSEQ_STORAGE_GROUP_DIR, "0" + File.separator + "0");

  @Override
  protected String getTestStorageGroup() {
    return TABLE_MODEL_TEST_SG;
  }

  @Override
  protected File getSeqStorageGroupDir() {
    return TABLE_MODEL_SEQ_STORAGE_GROUP_DIR;
  }

  @Override
  protected File getUnseqStorageGroupDir() {
    return TABLE_MODEL_UNSEQ_STORAGE_GROUP_DIR;
  }

  @Override
  protected File getSeqDirs() {
    return TABLE_MODEL_SEQ_DIRS;
  }

  @Override
  protected File getUnseqDirs() {
    return TABLE_MODEL_UNSEQ_DIRS;
  }
}
