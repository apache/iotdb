package org.apache.iotdb.flink.sql.exception;

public class UnsupportedDataTypeException extends RuntimeException{
    public UnsupportedDataTypeException(String s) {
        super(s);
    }
}
