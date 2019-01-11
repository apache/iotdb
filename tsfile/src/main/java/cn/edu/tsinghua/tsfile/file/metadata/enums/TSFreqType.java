package cn.edu.tsinghua.tsfile.file.metadata.enums;

public enum TSFreqType {

    SINGLE_FREQ, MULTI_FREQ, IRREGULAR_FREQ;

    public static TSFreqType deserialize(short i){
        switch (i) {
            case 0: return SINGLE_FREQ;
            case 1: return MULTI_FREQ;
            case 2: return IRREGULAR_FREQ;
            default: return IRREGULAR_FREQ;
        }
    }

    public short serialize(){
        switch (this) {
            case SINGLE_FREQ: return 0;
            case MULTI_FREQ: return 1;
            case IRREGULAR_FREQ: return 2;
            default: return 2;
        }
    }
}