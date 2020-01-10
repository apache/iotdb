package org.apache.iotdb.calcite;


import java.util.Map;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

/**
 * Factory that creates a {@link IoTDBSchema}
 */
public class IoTDBSchemaFactory implements SchemaFactory {

  public IoTDBSchemaFactory() {
  }

  @Override
  public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {

    final String host = (String) operand.get("host");
    final String userName = (String) operand.get("userName");
    final String password = (String) operand.get("password");

    int port = 6667;
    if (operand.containsKey("port")) {
      Object portObj = operand.get("port");

      if (portObj instanceof String) {
        port = Integer.parseInt((String) portObj);
      } else {
        port = (int) portObj;
      }
    }
    return new IoTDBSchema(host, port, userName, password, parentSchema, name);

  }
}

// End IoTDBSchemaFactory.java