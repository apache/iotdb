package org.apache.iotdb.flink.sql.provider;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.types.RowKind;
import org.apache.iotdb.flink.sql.function.IoTDBSinkFunction;
import org.apache.iotdb.flink.sql.wrapper.SchemaWrapper;
import org.apache.iotdb.rpc.IoTDBConnectionException;

public class IoTDBDynamicTableSink  implements DynamicTableSink {
    private final ReadableConfig options;
    private final TableSchema schema;

    public IoTDBDynamicTableSink(ReadableConfig options, TableSchema schema) {
        this.options = options;
        this.schema = schema;
    }


    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        try {
            return SinkFunctionProvider.of(new IoTDBSinkFunction(options, new SchemaWrapper(schema)));
        } catch (IoTDBConnectionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DynamicTableSink copy() {
        return null;
    }

    @Override
    public String asSummaryString() {
        return null;
    }
}
