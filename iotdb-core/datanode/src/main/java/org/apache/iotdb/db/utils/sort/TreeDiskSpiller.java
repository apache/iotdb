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

package org.apache.iotdb.db.utils.sort;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;

import java.util.List;

public class TreeDiskSpiller extends DiskSpiller {

  public TreeDiskSpiller(String folderPath, String filePrefix, List<TSDataType> dataTypeList) {
    super(folderPath, filePrefix, dataTypeList);
  }

  @Override
  protected TsBlock buildSortedTsBlock(TsBlockBuilder resultBuilder) {
    return resultBuilder.build();
  }

  @Override
  protected void appendTime(ColumnBuilder timeColumnBuilder, long time) {
    timeColumnBuilder.writeLong(time);
  }
}
