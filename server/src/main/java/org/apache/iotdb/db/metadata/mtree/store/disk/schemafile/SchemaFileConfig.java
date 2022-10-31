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
package org.apache.iotdb.db.metadata.mtree.store.disk.schemafile;

import org.apache.iotdb.db.conf.IoTDBDescriptor;

/** All public configs about SchemaFile is stated here. */
public class SchemaFileConfig {

  // region SchemaFile Configuration

  // current version of schema file
  public static final int SCHEMA_FILE_VERSION = 1;

  // folder to store .pst files
  public static String SCHEMA_FOLDER = IoTDBDescriptor.getInstance().getConfig().getSchemaDir();

  public static int FILE_HEADER_SIZE = 256; // size of file header in bytes

  public static final int PAGE_CACHE_SIZE =
      IoTDBDescriptor.getInstance()
          .getConfig()
          .getPageCacheSizeInSchemaFile(); // size of page cache

  // size of page within one redo log, restricting log around 1GB
  public static final int SCHEMA_FILE_LOG_SIZE =
      IoTDBDescriptor.getInstance().getConfig().getSchemaFileLogSize();

  // marks to note the state of schema file log
  public static final byte SF_PREPARE_MARK = (byte) 0xfe;
  public static final byte SF_COMMIT_MARK = (byte) 0xff;

  // endregion

  // region Page Configuration

  public static final int PAGE_LENGTH = 16 * 1024; // 16 kib for default
  public static final long PAGE_INDEX_MASK = 0xffff_ffffL; // highest bit is not included
  public static final short PAGE_HEADER_SIZE = 32;

  // value of type flag of a schema page
  public static final int PAGE_HEADER_INDEX_OFFSET = 1; // offset of page index among page header
  public static final byte SEGMENTED_PAGE = 0x00;
  public static final byte INTERNAL_PAGE = 0x01;
  public static final byte ALIAS_PAGE = 0x02;

  // offset on the buffer of attributes about the page
  public static int COMP_POINTER_OFFSET_DIGIT = 16;
  public static long COMP_PTR_OFFSET_MASK = 0x7fffL;

  // endregion

  // region Segment Configuration

  public static final int SEG_HEADER_SIZE = 25; // in bytes
  public static final short SEG_OFF_DIG =
      2; // length of short, which is the type of segment offset and index
  public static final short SEG_MAX_SIZ = (short) (PAGE_LENGTH - PAGE_HEADER_SIZE - SEG_OFF_DIG);
  public static final short SEG_MIN_SIZ =
      IoTDBDescriptor.getInstance().getConfig().getMinimumSegmentInSchemaFile() > SEG_MAX_SIZ
          ? SEG_MAX_SIZ
          : IoTDBDescriptor.getInstance().getConfig().getMinimumSegmentInSchemaFile();

  public static final int SEG_INDEX_DIGIT = 16; // for type short in bits
  public static final long SEG_INDEX_MASK = 0xffffL; // help to translate address

  public static final short[] SEG_SIZE_METRIC = {20, 40, 75, 150, 300};

  // formula: (PAGE_LENGTH-PAGE_HEADER)/n - SEG_OFF_DIG
  public static final short[] SEG_SIZE_LST = {
    (PAGE_LENGTH - PAGE_HEADER_SIZE) / 16 - SEG_OFF_DIG,
    (PAGE_LENGTH - PAGE_HEADER_SIZE) / 8 - SEG_OFF_DIG,
    (PAGE_LENGTH - PAGE_HEADER_SIZE) / 4 - SEG_OFF_DIG,
    (PAGE_LENGTH - PAGE_HEADER_SIZE) / 2 - SEG_OFF_DIG,
    SEG_MAX_SIZ
  };

  // endregion

  // region Debug & Develop Configuration

  // segment split into 2 part with different entries
  public static final boolean INCLINED_SPLIT = true;
  // split may implement a fast bulk way
  public static final boolean BULK_SPLIT = true;

  public static boolean DETAIL_SKETCH = true; // whether sketch leaf segment in detail
  public static int INTERNAL_SPLIT_VALVE = 0; // internal segment split at this spare

  // endregion

}
