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

package org.apache.iotdb.db.storageengine.load.converter;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletRawReq;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.table.TsFileInsertionEventTableParser;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AstVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Objects;
import java.util.Optional;

public class LoadTableStatementDataTypeConvertExecutionVisitor
    extends AstVisitor<Optional<TSStatus>, String> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(LoadTableStatementDataTypeConvertExecutionVisitor.class);

  @FunctionalInterface
  public interface StatementExecutor {
    // databaseName can NOT be null
    TSStatus execute(final Statement statement, final String databaseName);
  }

  private final StatementExecutor statementExecutor;

  public LoadTableStatementDataTypeConvertExecutionVisitor(StatementExecutor statementExecutor) {
    this.statementExecutor = statementExecutor;
  }

  @Override
  public Optional<TSStatus> visitLoadTsFile(
      final LoadTsFile loadTsFileStatement, final String databaseName) {
    if (Objects.isNull(databaseName)) {
      LOGGER.warn(
          "Database name is unexpectedly null for LoadTsFileStatement: {}. Skip data type conversion.",
          loadTsFileStatement);
      return Optional.empty();
    }

    LOGGER.info("Start data type conversion for LoadTsFileStatement: {}.", loadTsFileStatement);

    for (final File file : loadTsFileStatement.getTsFiles()) {
      try (final TsFileInsertionEventTableParser parser =
          new TsFileInsertionEventTableParser(
              file,
              new TablePattern(true, null, null),
              Long.MIN_VALUE,
              Long.MAX_VALUE,
              null,
              null)) {
        for (final TabletInsertionEvent tabletInsertionEvent : parser.toTabletInsertionEvents()) {
          if (!(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
            continue;
          }
          final PipeRawTabletInsertionEvent rawTabletInsertionEvent =
              (PipeRawTabletInsertionEvent) tabletInsertionEvent;

          final LoadConvertedInsertTabletStatement statement =
              new LoadConvertedInsertTabletStatement(
                  PipeTransferTabletRawReq.toTPipeTransferRawReq(
                          rawTabletInsertionEvent.convertToTablet(),
                          rawTabletInsertionEvent.isAligned())
                      .constructStatement());

          TSStatus result;
          try {
            result =
                statement.accept(
                    LoadTsFileDataTypeConverter.STATEMENT_STATUS_VISITOR,
                    statementExecutor.execute(statement, databaseName));

            // Retry max 5 times if the write process is rejected
            for (int i = 0;
                i < 5
                    && result.getCode()
                        == TSStatusCode.LOAD_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode();
                i++) {
              Thread.sleep(100L * (i + 1));
              result =
                  statement.accept(
                      LoadTsFileDataTypeConverter.STATEMENT_STATUS_VISITOR,
                      statementExecutor.execute(statement, databaseName));
            }
          } catch (final Exception e) {
            if (e instanceof InterruptedException) {
              Thread.currentThread().interrupt();
            }
            result = statement.accept(LoadTsFileDataTypeConverter.STATEMENT_EXCEPTION_VISITOR, e);
          }

          if (!(result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
              || result.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()
              || result.getCode()
                  == TSStatusCode.LOAD_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())) {
            return Optional.empty();
          }
        }
      } catch (final Exception e) {
        LOGGER.warn(
            "Failed to convert data type for LoadTsFileStatement: {}.", loadTsFileStatement, e);
        return Optional.empty();
      }
    }

    if (loadTsFileStatement.isDeleteAfterLoad()) {
      loadTsFileStatement.getTsFiles().forEach(FileUtils::deleteQuietly);
    }

    LOGGER.info(
        "Data type conversion for LoadTsFileStatement {} is successful.", loadTsFileStatement);

    return Optional.of(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
  }
}
