package cn.edu.tsinghua.iotdb.service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import cn.edu.tsinghua.iotdb.metadata.ColumnSchema;
import cn.edu.tsinghua.service.rpc.thrift.TSColumnSchema;
import cn.edu.tsinghua.service.rpc.thrift.TSDataValue;
import cn.edu.tsinghua.service.rpc.thrift.TSQueryDataSet;
import cn.edu.tsinghua.service.rpc.thrift.TSRowRecord;
import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.RowRecord;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;

/**
 * Utils to convert between thrift format and TsFile format
 */
public class Utils {

    public static Map<String, List<TSColumnSchema>> convertAllSchema(Map<String, List<ColumnSchema>> allSchema) {
        if (allSchema == null) {
            return null;
        }
        Map<String, List<TSColumnSchema>> tsAllSchema = new HashMap<>();
        for (Map.Entry<String, List<ColumnSchema>> entry : allSchema.entrySet()) {
            List<TSColumnSchema> tsColumnSchemas = new ArrayList<>();
            for (ColumnSchema columnSchema : entry.getValue()) {
                tsColumnSchemas.add(convertColumnSchema(columnSchema));
            }
            tsAllSchema.put(entry.getKey(), tsColumnSchemas);
        }
        return tsAllSchema;
    }

    private static TSColumnSchema convertColumnSchema(ColumnSchema schema) {
        if (schema == null) {
            return null;
        }
        TSColumnSchema tsColumnSchema = new TSColumnSchema();
        tsColumnSchema.setName(schema.name);
        tsColumnSchema.setDataType(schema.dataType == null ? null : schema.dataType.toString());
        tsColumnSchema.setEncoding(schema.encoding == null ? null : schema.encoding.toString());
        tsColumnSchema.setOtherArgs(schema.getArgsMap() == null ? null : schema.getArgsMap());
        return tsColumnSchema;
    }

	public static TSQueryDataSet convertQueryDataSetByFetchSize(QueryDataSet queryDataSet, int fetchsize) throws IOException {
		TSQueryDataSet tsQueryDataSet = new TSQueryDataSet();
		tsQueryDataSet.setRecords(new ArrayList<>());
		for (int i = 0; i < fetchsize; i++) {
			if (queryDataSet.hasNext()) {
				RowRecord rowRecord = queryDataSet.next();
				tsQueryDataSet.getRecords().add(convertToTSRecord(rowRecord));
			} else {
				break;
			}
		}
		return tsQueryDataSet;
	}

	public static TSRowRecord convertToTSRecord(RowRecord rowRecord) {
		TSRowRecord tsRowRecord = new TSRowRecord();
		tsRowRecord.setTimestamp(rowRecord.getTimestamp());
		tsRowRecord.setKeys(new ArrayList<>());
		tsRowRecord.setValues(new ArrayList<>());

		LinkedHashMap<Path, TsPrimitiveType> fields = rowRecord.getFields();
		for (Entry<Path, TsPrimitiveType> entry : fields.entrySet()) {
			tsRowRecord.getKeys().add(entry.getKey().toString());
			TsPrimitiveType type = entry.getValue();
			TSDataValue value = new TSDataValue(false);
			if (type == null) {
				value.setIs_empty(true);
			} else {
				switch (type.getDataType()) {
				case BOOLEAN:
					value.setBool_val(type.getBoolean());
					break;
				case INT32:
					value.setInt_val(type.getInt());
					break;
				case INT64:
					value.setLong_val(type.getLong());
					break;
				case FLOAT:
					value.setFloat_val(type.getFloat());
					break;
				case DOUBLE:
					value.setDouble_val(type.getDouble());
					break;
				case TEXT:
					value.setBinary_val(ByteBuffer.wrap(type.getBinary().values));
					break;
				default:
					throw new UnSupportedDataTypeException(String.format("data type %s is not supported when convert data at server", type.getDataType().toString()));
				}
				value.setType(type.getDataType().toString());
			}
			tsRowRecord.getValues().add(value);
		}
		return tsRowRecord;
	}
}
