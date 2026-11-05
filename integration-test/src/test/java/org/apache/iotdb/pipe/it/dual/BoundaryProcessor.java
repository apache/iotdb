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

package org.apache.iotdb.pipe.it.dual;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.row.PipeResetTabletRow;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.annotation.TreeModel;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Base64;

/** A stateless IT plugin: audit zero-output inputs, or derive two outputs from each input. */
@TreeModel
public class BoundaryProcessor implements PipeProcessor {
  private boolean fanOut;
  private Path auditFile;

  @Override
  public void validate(final PipeParameterValidator validator) {}

  @Override
  public void customize(
      final PipeParameters parameters, final PipeProcessorRuntimeConfiguration configuration) {
    fanOut = parameters.getBooleanOrDefault("fan-out", false);
    auditFile = Path.of(parameters.getString("audit-file"));
  }

  @Override
  public void process(final TabletInsertionEvent event, final EventCollector collector)
      throws Exception {
    if (fanOut) {
      // A single conversion marks only its final output as reporting the input's progress.
      // Resetting the tablet keeps the outputs distinct without assigning two progress owners.
      for (final TabletInsertionEvent output :
          event.processRowByRow(
              (row, rows) -> {
                for (final String suffix : new String[] {"first", "second"}) {
                  collectRow(
                      rows,
                      new PipeResetTabletRow(
                          0,
                          row.getDeviceId() + "_" + suffix,
                          false,
                          new MeasurementSchema[] {new MeasurementSchema("s1", TSDataType.INT32)},
                          new long[] {row.getTime()},
                          new TSDataType[] {TSDataType.INT32},
                          new Object[] {new int[] {row.getInt(0)}},
                          new BitMap[] {new BitMap(1)},
                          new String[] {"s1"}));
                }
              })) {
        collector.collect(output);
      }
      auditProgress(event, "parent-progress:");
    } else {
      for (final TabletInsertionEvent output :
          event.processRowByRow(
              (row, rows) -> {
                if (row.getInt(0) >= 0) {
                  collectRow(rows, row);
                } else {
                  try {
                    auditProgress(
                        event, "filtered-progress:" + row.getTime() + "," + row.getInt(0) + ":");
                    Files.writeString(
                        auditFile,
                        "filtered:" + row.getTime() + "," + row.getInt(0) + "\n",
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND);
                  } catch (final IOException e) {
                    throw new UncheckedIOException(e);
                  }
                }
              })) {
        collector.collect(output);
      }
    }
    // The durable audit proves the input reached this processor. It is never used to gate or
    // acknowledge production events and is deliberately outside production source directories.
    Files.writeString(
        auditFile, "processed\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND);
  }

  private void auditProgress(final TabletInsertionEvent event, final String prefix)
      throws IOException {
    final ByteArrayOutputStream progress = new ByteArrayOutputStream();
    ((EnrichedEvent) event).getProgressIndex().serialize(progress);
    Files.writeString(
        auditFile,
        prefix + Base64.getEncoder().encodeToString(progress.toByteArray()) + "\n",
        StandardOpenOption.CREATE,
        StandardOpenOption.APPEND);
  }

  private static void collectRow(final RowCollector collector, final Row row) {
    try {
      collector.collectRow(row);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void process(final TsFileInsertionEvent event, final EventCollector collector)
      throws Exception {
    try {
      for (final TabletInsertionEvent tablet : event.toTabletInsertionEvents()) {
        process(tablet, collector);
      }
    } finally {
      event.close();
    }
  }

  @Override
  public void process(final Event event, final EventCollector collector) throws Exception {
    collector.collect(event);
  }

  @Override
  public void close() {}
}
