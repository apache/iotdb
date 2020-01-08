package org.apache.iotdb.calcite;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.linq4j.tree.Types;

import java.lang.reflect.Method;
import java.util.List;

/**
 * Builtin methods in the IoTDB adapter.
 */
public enum IoTDBMethod {
  IoTDB_QUERYABLE_QUERY(IoTDBTable.IoTDBQueryable.class, "query",
          List.class, List.class, List.class, List.class, Integer.class, Integer.class);

  public final Method method;

  public static final ImmutableMap<Method, IoTDBMethod> MAP;

  static {
    final ImmutableMap.Builder<Method, IoTDBMethod> builder =
            ImmutableMap.builder();
    for (IoTDBMethod value : IoTDBMethod.values()) {
      builder.put(value.method, value);
    }
    MAP = builder.build();
  }

  IoTDBMethod(Class clazz, String methodName, Class... argumentTypes) {
    this.method = Types.lookupMethod(clazz, methodName, argumentTypes);
  }
}

// End IoTDBMethod.java

