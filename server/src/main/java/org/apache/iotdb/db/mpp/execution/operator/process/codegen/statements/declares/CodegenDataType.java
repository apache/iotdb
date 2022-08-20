package org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.declares;

public enum CodegenDataType {
  DOUBLE {
    @Override
    public String toString() {
      return "Double";
    }
  },
  FLOAT {
    @Override
    public String toString() {
      return "Float";
    }
  },
  INT {
    @Override
    public String toString() {
      return "Integer";
    }
  },
  LONG {
    @Override
    public String toString() {
      return "Long";
    }
  },
  BOOLEAN {
    @Override
    public String toString() {
      return "Boolean";
    }
  },
  STRING {
    @Override
    public String toString() {
      return "String";
    }
  }
}
