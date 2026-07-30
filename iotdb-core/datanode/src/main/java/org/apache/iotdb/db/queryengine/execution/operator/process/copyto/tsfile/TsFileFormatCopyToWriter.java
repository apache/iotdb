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

package org.apache.iotdb.db.queryengine.execution.operator.process.copyto.tsfile;

import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.execution.operator.process.copyto.IFormatCopyToWriter;

import org.apache.ratis.util.MemoizedCheckedSupplier;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.ColumnSchema;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.v4.TableTsBlock2TsFileWriter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TsFileFormatCopyToWriter implements IFormatCopyToWriter {
  private static final String AUTO_GEN_MARK = "(auto_gen)";
  private final File targetFile;
  private final String targetTableName;
  private final String targetTimeColumn;
  private final Set<String> targetTagColumns;
  private final boolean generateNewTimeColumn;

  private MemoizedCheckedSupplier<TableTsBlock2TsFileWriter, IOException> tsFileWriter;
  private long rowCount = 0;
  private long deviceCount = 0;

  public TsFileFormatCopyToWriter(
      File file,
      CopyToTsFileOptions copyToOptions,
      List<ColumnHeader> innerQueryDatasetHeader,
      int[] columnIndex2TsBlockColumnIndex) {
    this.targetFile = file;
    this.targetTableName = copyToOptions.getTargetTableName();
    this.targetTimeColumn = copyToOptions.getTargetTimeColumn();
    this.generateNewTimeColumn = copyToOptions.isGenerateNewTimeColumn();
    targetTagColumns = copyToOptions.getTargetTagColumns();

    List<ColumnSchema> columnSchemas =
        new ArrayList<>(innerQueryDatasetHeader.size() + (generateNewTimeColumn ? 1 : 0));
    Map<String, Integer> columnName2TsBlockIndexMap = new HashMap<>();
    Map<String, ColumnHeader> columnHeaderMap = new HashMap<>();
    for (int i = 0; i < innerQueryDatasetHeader.size(); i++) {
      ColumnHeader columnHeader = innerQueryDatasetHeader.get(i);
      columnName2TsBlockIndexMap.put(
          columnHeader.getColumnName(), columnIndex2TsBlockColumnIndex[i]);
      columnHeaderMap.put(columnHeader.getColumnName(), columnHeader);
    }
    // add time column
    columnSchemas.add(
        new ColumnSchema(targetTimeColumn, TSDataType.TIMESTAMP, ColumnCategory.TIME));
    int timeColumnIdxInQueryTsBlock =
        generateNewTimeColumn ? -1 : columnName2TsBlockIndexMap.get(targetTimeColumn);

    // add tag columns
    int[] tagColumnIndexesInTsBlock = new int[targetTagColumns.size()];
    int arrIdx = 0;
    for (String targetTagColumn : targetTagColumns) {
      int tagIdx = columnName2TsBlockIndexMap.get(targetTagColumn);
      ColumnHeader tagColumnHeader = columnHeaderMap.get(targetTagColumn);
      columnSchemas.add(
          new ColumnSchema(
              tagColumnHeader.getColumnName(),
              tagColumnHeader.getColumnType(),
              ColumnCategory.TAG));
      tagColumnIndexesInTsBlock[arrIdx++] = tagIdx;
    }
    // add field columns
    int fieldColumnCount =
        innerQueryDatasetHeader.size()
            - tagColumnIndexesInTsBlock.length
            - (generateNewTimeColumn ? 0 : 1);
    int[] fieldColumnIndexesInQueryTsBlock = new int[fieldColumnCount];
    IMeasurementSchema[] fieldColumnSchemas = new IMeasurementSchema[fieldColumnCount];
    arrIdx = 0;
    for (ColumnHeader columnHeader : innerQueryDatasetHeader) {
      String columnName = columnHeader.getColumnName();
      if (targetTagColumns.contains(columnName) || columnName.equals(targetTimeColumn)) {
        continue;
      }
      columnSchemas.add(
          new ColumnSchema(columnName, columnHeader.getColumnType(), ColumnCategory.FIELD));
      fieldColumnSchemas[arrIdx] = new MeasurementSchema(columnName, columnHeader.getColumnType());
      fieldColumnIndexesInQueryTsBlock[arrIdx] = columnName2TsBlockIndexMap.get(columnName);
      arrIdx++;
    }

    TableSchema tableSchema = new TableSchema(targetTableName, columnSchemas);
    this.tsFileWriter =
        MemoizedCheckedSupplier.valueOf(
            () ->
                new TableTsBlock2TsFileWriter(
                    targetFile,
                    tableSchema,
                    copyToOptions.getTargetMemoryThreshold(),
                    generateNewTimeColumn,
                    timeColumnIdxInQueryTsBlock,
                    tagColumnIndexesInTsBlock,
                    fieldColumnIndexesInQueryTsBlock,
                    fieldColumnSchemas));
  }

  @Override
  public void write(TsBlock tsBlock) throws Exception {
    tsFileWriter.get().write(tsBlock);
  }

  @Override
  public void seal() throws Exception {
    if (!tsFileWriter.isInitialized()) {
      return;
    }
    TableTsBlock2TsFileWriter writer = tsFileWriter.get();
    writer.close();
    // should call these methods after writer.close()
    deviceCount = writer.getDeviceCount();
    rowCount = writer.getRowCount();
    tsFileWriter = null;
  }

  @Override
  public TsBlock buildResultTsBlock() {
    TsBlockBuilder builder =
        TsBlockBuilder.withMaxTsBlockSize(
            1024,
            ColumnHeaderConstant.COPY_TO_TSFILE_COLUMN_HEADERS.stream()
                .map(ColumnHeader::getColumnType)
                .collect(Collectors.toList()));
    builder.getTimeColumnBuilder().writeLong(0);
    builder.getValueColumnBuilders()[0].writeBinary(
        new Binary(rowCount > 0 ? targetFile.getAbsolutePath() : "", TSFileConfig.STRING_CHARSET));
    builder.getValueColumnBuilders()[1].writeLong(rowCount);
    builder.getValueColumnBuilders()[2].writeLong(deviceCount);
    builder.getValueColumnBuilders()[3].writeLong(targetFile.length());
    builder.getValueColumnBuilders()[4].writeBinary(
        new Binary(targetTableName, TSFileConfig.STRING_CHARSET));
    builder.getValueColumnBuilders()[5].writeBinary(
        new Binary(
            targetTimeColumn + (generateNewTimeColumn ? AUTO_GEN_MARK : ""),
            TSFileConfig.STRING_CHARSET));
    builder.getValueColumnBuilders()[6].writeBinary(
        new Binary(targetTagColumns.toString(), TSFileConfig.STRING_CHARSET));
    builder.declarePosition();
    return builder.build();
  }

  @Override
  public void close() throws IOException {
    if (tsFileWriter == null || !tsFileWriter.isInitialized()) {
      return;
    }
    tsFileWriter.get().close();
  }
}
