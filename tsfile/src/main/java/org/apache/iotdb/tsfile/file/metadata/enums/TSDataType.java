package org.apache.iotdb.tsfile.file.metadata.enums;

public enum TSDataType {
    BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT;

    public static TSDataType deserialize(short i){
        assert i <6;
        switch (i){
            case 0: return BOOLEAN;
            case 1: return INT32;
            case 2: return INT64;
            case 3: return FLOAT;
            case 4: return DOUBLE;
            case 5: return TEXT;
            default: return TEXT;
        }
    }
    public short serialize(){
        switch (this){
            case BOOLEAN: return 0;
            case INT32: return 1;
            case INT64: return  2;
            case FLOAT: return 3;
            case DOUBLE: return 4;
            case TEXT: return 5;
            default: return -1;
        }
    }
    public static int getSerializedSize(){ return Short.BYTES;}
}
