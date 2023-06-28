package org.apache.iotdb.flink.sql.factory;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.iotdb.flink.sql.common.Options;
import org.apache.iotdb.flink.sql.common.Utils;
import org.apache.iotdb.flink.sql.exception.*;
import org.apache.iotdb.flink.sql.provider.IoTDBDynamicTableSink;
import org.apache.iotdb.flink.sql.provider.IoTDBDynamicTableSource;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class IoTDBDynamicTableFactor implements DynamicTableSourceFactory, DynamicTableSinkFactory {
    private final HashSet<DataType> supportedDataTypes = new HashSet<DataType>(){{
        add(DataTypes.INT());
        add(DataTypes.BIGINT());
        add(DataTypes.FLOAT());
        add(DataTypes.DOUBLE());
        add(DataTypes.BOOLEAN());
        add(DataTypes.STRING());
    }};

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        ReadableConfig options = helper.getOptions();
        TableSchema schema = context.getCatalogTable().getSchema();

        validate(options, schema);

        return new IoTDBDynamicTableSource(options, schema);
    }

    @Override
    public String factoryIdentifier() {
        return "IoTDB";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        HashSet<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(Options.DEVICE);

        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        HashSet<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(Options.NODE_URLS);
        optionalOptions.add(Options.USER);
        optionalOptions.add(Options.PASSWORD);
        optionalOptions.add(Options.LOOKUP_CACHE_MAX_ROWS);
        optionalOptions.add(Options.LOOKUP_CACHE_TTL_SEC);
        optionalOptions.add(Options.ALIGNED);

        return optionalOptions;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        ReadableConfig options = helper.getOptions();
        TableSchema schema = context.getCatalogTable().getSchema();

        validate(options, schema);

        return new IoTDBDynamicTableSink(options, schema);
    }

    protected void validate(ReadableConfig options, TableSchema schema) {
        String[] fieldNames = schema.getFieldNames();
        DataType[] fieldDataTypes = schema.getFieldDataTypes();

        if (!"Time_".equals(fieldNames[0]) || !fieldDataTypes[0].equals(DataTypes.BIGINT())) {
            throw new IllegalSchemaException("The first field's name must be `Time_`, and its data type must be BIGINT.");
        }
        for (String fieldName : fieldNames) {
            if (fieldName.contains("\\.")) {
                throw new IllegalIoTDBPathException(String.format("The field name `%s` contains character `.`, it's not allowed in IoTDB.", fieldName));
            }
            if (Utils.isNumeric(fieldName)) {
                throw new IllegalIoTDBPathException(String.format("The field name `%s` is a purely number, it's not allowed in IoTDB.", fieldName));
            }
        }

        for (DataType fieldDataType : fieldDataTypes) {
            if (!supportedDataTypes.contains(fieldDataType)) {
                throw new UnsupportedDataTypeException("IoTDB don't support the data type: " + fieldDataType);
            }
        }

        String device = options.get(Options.DEVICE);
        if (!device.startsWith("root.")) {
            throw new IllegalIoTDBPathException("The option `device` must starts with 'root.'.");
        }
        for (String s : device.split("\\.")) {
            if (Utils.isNumeric(s)) {
                throw new IllegalIoTDBPathException(String.format("The option `device` contains a purely number path: `%s`, it's not allowed in IoTDB.", s));
            }
        }

        List<String> nodeUrls = Arrays.asList(options.get(Options.NODE_URLS).toString().split(","));
        for (String nodeUrl : nodeUrls) {
            nodeUrl = nodeUrl.strip();
            String[] split = nodeUrl.split(":");
            if (split.length != 2){
                throw new IllegalUrlPathException("Every node's URL must be in the format of `host:port`.");
            }
            if (!Utils.isNumeric(split[1]) && Integer.valueOf(split[1]) > 65535 && Integer.valueOf(split[1]) < 1) {
                throw new IllegalUrlPathException("The port must be a number, and it could not be greater than 65535 or less than 1.");
            }
        }

        Long lowerBound = options.get(Options.SCAN_BOUNDED_LOWER_BOUND);
        Long upperBound = options.get(Options.SCAN_BOUNDED_UPPER_BOUND);
        if (lowerBound > 0L && upperBound > 0L && upperBound < lowerBound) {
            throw new IllegalOptionException("The value of option `scan.bounded.lower-bound` could not be greater than the value of option `scan.bounded.upper-bound`.");
        }
    }
}
