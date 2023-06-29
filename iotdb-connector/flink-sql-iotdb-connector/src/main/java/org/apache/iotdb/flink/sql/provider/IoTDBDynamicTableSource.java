package org.apache.iotdb.flink.sql.provider;

import org.apache.iotdb.flink.sql.function.IoTDBBoundedScanFunction;
import org.apache.iotdb.flink.sql.function.IoTDBLookupFunction;
import org.apache.iotdb.flink.sql.wrapper.SchemaWrapper;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;

public class IoTDBDynamicTableSource implements LookupTableSource, ScanTableSource {
  private final ReadableConfig options;
  private final TableSchema schema;

  public IoTDBDynamicTableSource(ReadableConfig options, TableSchema schema) {
    this.options = options;
    this.schema = schema;
  }

  @Override
  public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {
    return TableFunctionProvider.of(new IoTDBLookupFunction(options, new SchemaWrapper(schema)));
  }

  @Override
  public DynamicTableSource copy() {
    return new IoTDBDynamicTableSource(options, schema);
  }

  @Override
  public String asSummaryString() {
    return "IoTDB Dynamic Table Source";
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
    return InputFormatProvider.of(new IoTDBBoundedScanFunction(options, new SchemaWrapper(schema)));
  }
}
