package cn.edu.thu.tsfiledb.utils;

import java.util.List;
import java.util.Map.Entry;

import org.json.JSONArray;
import org.json.JSONObject;

import cn.edu.thu.tsfile.common.constant.JsonFormatConstant;
import cn.edu.thu.tsfile.timeseries.write.exception.InvalidJsonSchemaException;
import cn.edu.thu.tsfile.timeseries.write.schema.FileSchema;
import cn.edu.thu.tsfiledb.metadata.ColumnSchema;

public class FileSchemaUtil {
	
	 public static FileSchema getFileSchemaFromColumnSchema(List<ColumnSchema> schemaList, String measureType) {
	        JSONObject jsonSchema = new JSONObject();
	        JSONArray rowGroupArray = new JSONArray();
	        
	        for (ColumnSchema col : schemaList) {
	            JSONObject s1 = new JSONObject();
	            s1.put(JsonFormatConstant.MEASUREMENT_UID, col.name);
	            s1.put(JsonFormatConstant.DATA_TYPE, col.dataType.toString());
	            s1.put(JsonFormatConstant.MEASUREMENT_ENCODING, col.encoding.toString());
	            for (Entry<String, String> entry : col.getArgsMap().entrySet()) {
	                if (JsonFormatConstant.ENUM_VALUES.equals(entry.getKey())) {
	                    String[] valueArray = entry.getValue().split(",");
	                    s1.put(JsonFormatConstant.ENUM_VALUES, new JSONArray(valueArray));
	                } else
	                    s1.put(entry.getKey(), entry.getValue().toString());
	            }
	            rowGroupArray.put(s1);
	        }

	        jsonSchema.put(JsonFormatConstant.DELTA_TYPE, measureType);
	        jsonSchema.put(JsonFormatConstant.JSON_SCHEMA, rowGroupArray);
	        FileSchema fileSchema = null;
			try {
				fileSchema = new FileSchema(jsonSchema);
			} catch (InvalidJsonSchemaException e) {
				//This exception won't occur
				e.printStackTrace();
			}
	        return fileSchema;
	    }

}
