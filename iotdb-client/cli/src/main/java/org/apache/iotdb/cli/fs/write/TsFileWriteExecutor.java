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

package org.apache.iotdb.cli.fs.write;

import org.apache.iotdb.cli.fs.FilesystemShell;
import org.apache.iotdb.cli.fs.command.WriteOptions;
import org.apache.iotdb.cli.i18n.CliMessages;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.v4.ITsFileWriter;
import org.apache.tsfile.write.v4.TsFileWriterBuilder;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import java.io.FilterReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/** Streams strict CSV into a new local TsFile and publishes it without overwriting a target. */
public final class TsFileWriteExecutor {
  private static final int BATCH_SIZE = 1024;

  public int run(WriteOptions options, InputStream stdin, PrintStream err) {
    return run(options, utf8Reader(stdin), err);
  }

  public int run(WriteOptions options, Reader stdin, PrintStream err) {
    TableSchema schema;
    try {
      schema = schema(options);
    } catch (IllegalArgumentException e) {
      diagnostic(err, e.getMessage());
      return FilesystemShell.USAGE_ERROR;
    }
    Path temporary = null;
    int status = FilesystemShell.SUCCESS;
    long rowCount = 0;
    try {
      Path target = Paths.get(options.getOutput()).toAbsolutePath();
      // Validate and open the CSV header before creating any output, as TsFile-Cli does.
      try (Reader reader = openInput(options, stdin)) {
        StrictCsvReader csv;
        try {
          csv = new StrictCsvReader(reader, options);
        } catch (IOException e) {
          throw new InputFailure(e);
        }
        if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
          throw new IOException(
              String.format(
                  CliMessages.EXCEPTION_OUTPUT_TARGET_ALREADY_EXISTS_ARG_238A4D83,
                  options.getOutput()));
        }
        temporary = Files.createTempFile(target.getParent(), ".iotdb-write-", ".tsfile");
        try (ITsFileWriter writer =
            new TsFileWriterBuilder().file(temporary.toFile()).tableSchema(schema).build()) {
          Tablet tablet =
              new Tablet(
                  schema.getTableName(),
                  IMeasurementSchema.getMeasurementNameList(schema.getColumnSchemas()),
                  IMeasurementSchema.getDataTypeList(schema.getColumnSchemas()),
                  schema.getColumnTypes(),
                  BATCH_SIZE);
          StrictCsvReader.Row row;
          while ((row = next(csv)) != null) {
            int index = tablet.getRowSize();
            tablet.addTimestamp(index, row.getTimestamp());
            for (int column = 0; column < options.getColumns().size(); column++) {
              Object value = row.getValues().get(column);
              if (value instanceof byte[]) {
                value = new Binary((byte[]) value);
              }
              tablet.addValue(options.getColumns().get(column).getName(), index, value);
            }
            if (tablet.getRowSize() == BATCH_SIZE) {
              writer.write(tablet);
              tablet.reset();
            }
          }
          if (tablet.getRowSize() > 0) {
            writer.write(tablet);
          }
        }
        rowCount = csv.getRowCount();
      }
      // Creating a hard link is atomic and fails if another process already created the target.
      // Files.move(ATOMIC_MOVE) may replace existing targets even without REPLACE_EXISTING.
      Files.createLink(target, temporary);
    } catch (InputFailure e) {
      diagnostic(err, e.getMessage());
      status = FilesystemShell.INPUT_ERROR;
    } catch (IOException | WriteProcessException | RuntimeException e) {
      diagnostic(
          err,
          String.format(
              CliMessages.MESSAGE_CANNOT_WRITE_OUTPUT_ARG_ARG_AB420A33,
              options.getOutput(),
              e.getMessage()));
      status = FilesystemShell.RUNTIME_ERROR;
    } finally {
      if (temporary != null) {
        try {
          Files.deleteIfExists(
              Paths.get(temporary + TsFileIOWriter.CHUNK_METADATA_TEMP_FILE_SUFFIX));
          Files.deleteIfExists(temporary);
        } catch (IOException e) {
          diagnostic(
              err,
              String.format(
                  CliMessages.MESSAGE_CANNOT_REMOVE_TEMPORARY_OUTPUT_ARG_ARG_187E734C,
                  temporary,
                  e.getMessage()));
          status = FilesystemShell.RUNTIME_ERROR;
        }
      }
    }
    if (status == FilesystemShell.SUCCESS && options.isVerbose()) {
      verbose(options, schema, rowCount, err);
      if (err.checkError()) {
        return FilesystemShell.RUNTIME_ERROR;
      }
    }
    return status;
  }

  private static StrictCsvReader.Row next(StrictCsvReader csv) throws InputFailure {
    try {
      return csv.next();
    } catch (IOException e) {
      throw new InputFailure(e);
    }
  }

  private static Reader openInput(WriteOptions options, Reader stdin) throws InputFailure {
    try {
      if (options.isStdin()) {
        return new FilterReader(stdin) {
          @Override
          public void close() {
            // Standard input belongs to the enclosing CLI session.
          }
        };
      }
      Path input = Paths.get(options.getInput());
      if (!Files.isRegularFile(input)) {
        throw new IOException(
            String.format(
                CliMessages.EXCEPTION_INPUT_MUST_BE_A_REGULAR_CSV_FILE_ARG_AEB02C75,
                options.getInput()));
      }
      return utf8Reader(Files.newInputStream(input));
    } catch (IOException | RuntimeException e) {
      throw new InputFailure(e);
    }
  }

  private static Reader utf8Reader(InputStream input) {
    return new InputStreamReader(
        input,
        StandardCharsets.UTF_8
            .newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT));
  }

  private static TableSchema schema(WriteOptions options) {
    List<IMeasurementSchema> columns = new ArrayList<>();
    List<ColumnCategory> categories = new ArrayList<>();
    for (WriteOptions.Column column : options.getColumns()) {
      TSDataType type = TSDataType.valueOf(column.getType());
      String compressionName = options.getCompressions().getOrDefault(column.getType(), "LZ4");
      CompressionType compression;
      try {
        compression = CompressionType.valueOf(compressionName);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format(
                CliMessages
                    .EXCEPTION_COMPRESSION_ARG_IS_NOT_SUPPORTED_BY_THE_BUNDLED_TSFILE_WRITER_03C0806D,
                compressionName));
      }
      TSEncoding encoding =
          TSEncoding.valueOf(
              options.getEncodings().getOrDefault(column.getType(), defaultEncoding(type).name()));
      if (!TSEncoding.isSupported(type, encoding)) {
        throw new IllegalArgumentException(
            String.format(
                CliMessages.EXCEPTION_ENCODING_ARG_IS_NOT_SUPPORTED_FOR_DATA_TYPE_ARG_218D4FB0,
                encoding,
                type));
      }
      columns.add(new MeasurementSchema(column.getName(), type, encoding, compression));
      categories.add(ColumnCategory.valueOf(column.getCategory()));
    }
    TableSchema schema = new TableSchema(options.getTable(), columns, categories);
    validateResolvedName(options.getTable(), schema.getTableName());
    for (int i = 0; i < options.getColumns().size(); i++) {
      validateResolvedName(
          options.getColumns().get(i).getName(),
          schema.getColumnSchemas().get(i).getMeasurementName());
    }
    return schema;
  }

  private static void validateResolvedName(String declared, String resolved) {
    if (!declared.equals(resolved)) {
      throw new IllegalArgumentException(
          String.format(
              CliMessages
                  .EXCEPTION_NAME_ARG_CANNOT_BE_PRESERVED_BY_THE_BUNDLED_TSFILE_WRITER_RESOLVED_AS_ARG_697B06A9,
              declared,
              resolved));
    }
  }

  private static TSEncoding defaultEncoding(TSDataType type) {
    switch (type) {
      case INT32:
      case INT64:
      case DATE:
      case TIMESTAMP:
        return TSEncoding.TS_2DIFF;
      case FLOAT:
      case DOUBLE:
        return TSEncoding.GORILLA;
      default:
        return TSEncoding.PLAIN;
    }
  }

  private static void verbose(
      WriteOptions options, TableSchema schema, long rows, PrintStream err) {
    err.println(
        String.format(
            CliMessages.MESSAGE_CREATED_MODEL_TABLE_OBJECT_ARG_ROWS_ARG_OUTPUT_ARG_70FEA8AE,
            options.getTable(),
            rows,
            options.getOutput()));
    for (int i = 0; i < options.getColumns().size(); i++) {
      WriteOptions.Column column = options.getColumns().get(i);
      IMeasurementSchema resolved = schema.getColumnSchemas().get(i);
      err.println(
          String.format(
              CliMessages
                  .MESSAGE_COLUMN_ARG_CATEGORY_ARG_DATA_TYPE_ARG_ENCODING_ARG_SOURCE_ARG_COMPRESSION_ARG_SOURCE_ARG_4AA9D567,
              column.getName(),
              column.getCategory(),
              column.getType(),
              resolved.getEncodingType(),
              options.getEncodings().containsKey(column.getType()) ? "type-override" : "default",
              resolved.getCompressor(),
              options.getCompressions().containsKey(column.getType())
                  ? "type-override"
                  : "default"));
    }
  }

  private static void diagnostic(PrintStream err, String message) {
    err.println(String.format(CliMessages.MESSAGE_ERROR_ARG_10E10A81, message));
  }

  private static final class InputFailure extends Exception {
    private InputFailure(Exception cause) {
      super(cause.getMessage(), cause);
    }
  }
}
