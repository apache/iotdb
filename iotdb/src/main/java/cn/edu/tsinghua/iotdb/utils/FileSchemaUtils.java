package cn.edu.tsinghua.iotdb.utils;

import java.util.List;
import java.util.Map.Entry;

import cn.edu.tsinghua.tsfile.exception.write.WriteProcessException;
import org.json.JSONArray;
import org.json.JSONObject;

import cn.edu.tsinghua.iotdb.metadata.ColumnSchema;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.tsfile.common.constant.JsonFormatConstant;
import cn.edu.tsinghua.tsfile.write.schema.FileSchema;

public class FileSchemaUtils {

	public static FileSchema constructFileSchema(String processorName) throws WriteProcessException {

		List<ColumnSchema> columnSchemaList;
		columnSchemaList = MManager.getInstance().getSchemaForFileName(processorName);

		FileSchema fileSchema = null;
		try {
			fileSchema = getFileSchemaFromColumnSchema(columnSchemaList, processorName);
		} catch (WriteProcessException e) {
			throw e;
		}
		return fileSchema;

	}

	public static FileSchema getFileSchemaFromColumnSchema(List<ColumnSchema> schemaList, String deviceType)
			throws WriteProcessException {
		JSONArray rowGroup = new JSONArray();

		for (ColumnSchema col : schemaList) {
			JSONObject measurement = new JSONObject();
			measurement.put(JsonFormatConstant.MEASUREMENT_UID, col.name);
			measurement.put(JsonFormatConstant.DATA_TYPE, col.dataType.toString());
			measurement.put(JsonFormatConstant.MEASUREMENT_ENCODING, col.encoding.toString());
			for (Entry<String, String> entry : col.getArgsMap().entrySet()) {
				if (JsonFormatConstant.ENUM_VALUES.equals(entry.getKey())) {
					String[] valueArray = entry.getValue().split(",");
					measurement.put(JsonFormatConstant.ENUM_VALUES, new JSONArray(valueArray));
				} else
					measurement.put(entry.getKey(), entry.getValue().toString());
			}
			rowGroup.put(measurement);
		}
		JSONObject jsonSchema = new JSONObject();
		jsonSchema.put(JsonFormatConstant.JSON_SCHEMA, rowGroup);
		jsonSchema.put(JsonFormatConstant.DELTA_TYPE, deviceType);
		return new FileSchema(jsonSchema);
	}

}
