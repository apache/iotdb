package org.apache.iotdb.tsfile.file.metadata.enums;

public enum TSEncoding {

    PLAIN, PLAIN_DICTIONARY, RLE, DIFF, TS_2DIFF, BITMAP, GORILLA;

    public static TSEncoding deserialize(short i){
        switch (i) {
            case 0: return PLAIN;
            case 1: return PLAIN_DICTIONARY;
            case 2: return RLE;
            case 3: return DIFF;
            case 4: return TS_2DIFF;
            case 5: return BITMAP;
            case 6: return GORILLA;
            default: return PLAIN;
        }
    }

    public short serialize(){
        switch (this) {
            case PLAIN: return 0;
            case PLAIN_DICTIONARY: return 1;
            case RLE: return 2;
            case DIFF: return 3;
            case TS_2DIFF: return 4;
            case BITMAP: return 5;
            case GORILLA: return 6;
            default: return 0;
        }
    }

    public static int getSerializedSize(){ return Short.BYTES;}
}
