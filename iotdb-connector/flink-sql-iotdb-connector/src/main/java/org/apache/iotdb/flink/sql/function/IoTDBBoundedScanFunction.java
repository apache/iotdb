package org.apache.iotdb.flink.sql.function;


import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.iotdb.flink.sql.common.Options;
import org.apache.iotdb.flink.sql.common.Utils;
import org.apache.iotdb.flink.sql.wrapper.SchemaWrapper;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class IoTDBBoundedScanFunction extends RichInputFormat<RowData, InputSplit> {
    private final ReadableConfig OPTIONS;
    private final List<Tuple2<String, DataType>> SCHEMA;
    private final String DEVICE;
    private final long LOWER_BOUND;
    private final long UPPER_BOUND;
    private final List<String> MEASUREMENTS;
    private Session session;
    private SessionDataSet dataSet;
    private List<String> columnTypes;

    public IoTDBBoundedScanFunction(ReadableConfig options, SchemaWrapper schemaWrapper) {
        OPTIONS = options;
        SCHEMA = schemaWrapper.getSchema();
        DEVICE = options.get(Options.DEVICE);
        LOWER_BOUND = options.get(Options.SCAN_BOUNDED_LOWER_BOUND);
        UPPER_BOUND = options.get(Options.SCAN_BOUNDED_UPPER_BOUND);
        MEASUREMENTS = SCHEMA.stream().map(field -> String.valueOf(field.f0)).collect(Collectors.toList());
    }

    @Override
    public void configure(Configuration configuration) {
        // fo nothing
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
        return baseStatistics;
    }

    @Override
    public InputSplit[] createInputSplits(int i) throws IOException {
        return new GenericInputSplit[] {new GenericInputSplit(1, 1)};
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    @Override
    public void openInputFormat() throws IOException {
        session = new Session.Builder()
                .nodeUrls(Arrays.asList(OPTIONS.get(Options.NODE_URLS).split(",")))
                .username(OPTIONS.get(Options.USER))
                .password(OPTIONS.get(Options.PASSWORD))
                .build();

        try {
            session.open(false);
        } catch (IoTDBConnectionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void open(InputSplit inputSplit) throws IOException {
        String sql;
        if (LOWER_BOUND < 0L && UPPER_BOUND < 0L) {
            sql = String.format("SELECT %s FROM %s", String.join(",", MEASUREMENTS), DEVICE);
        } else if (LOWER_BOUND < 0L && UPPER_BOUND > 0L) {
            sql = String.format("SELECT %s FROM %s WHERE TIME <= %d", String.join(",", MEASUREMENTS), DEVICE, UPPER_BOUND);
        } else if (LOWER_BOUND > 0L && UPPER_BOUND < 0L) {
            sql = String.format("SELECT %s FROM %s WHERE TIME >= %d", String.join(",", MEASUREMENTS), DEVICE, LOWER_BOUND);
        } else {
            sql = String.format("SELECT %s FROM %s WHERE TIME >= %d AND TIME <= %d", String.join(",", MEASUREMENTS), DEVICE, LOWER_BOUND, UPPER_BOUND);
        }
        try {
            dataSet = session.executeQueryStatement(sql);
            columnTypes = dataSet.getColumnTypes();
        } catch (StatementExecutionException | IoTDBConnectionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        try {
            return !dataSet.hasNext();
        } catch (StatementExecutionException | IoTDBConnectionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public RowData nextRecord(RowData rowData) throws IOException {
        try {
            RowRecord record = dataSet.next();
            return Utils.convert(record, columnTypes);
        } catch (StatementExecutionException | IoTDBConnectionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (dataSet != null) {
                dataSet.close();
            }
            if (session != null) {
                session.close();
            }
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
