package org.apache.iotdb.tsfile.file.metadata.enums;

import org.apache.iotdb.tsfile.exception.compress.CompressionTypeNotSupportedException;

public enum CompressionType {
        UNCOMPRESSED,
        SNAPPY,
        GZIP,
        LZO,
        SDT,
        PAA,
        PLA;

        public static CompressionType deserialize(short i){
            switch (i) {
                case 0: return UNCOMPRESSED;
                case 1: return SNAPPY;
                case 2: return GZIP;
                case 3: return LZO;
                case 4: return SDT;
                case 5: return PAA;
                case 6: return PLA;
                default: return UNCOMPRESSED;
            }
        }

        public short serialize(){
            switch (this) {
                case UNCOMPRESSED: return 0;
                case SNAPPY: return 1;
                case GZIP: return 2;
                case LZO: return 3;
                case SDT: return 4;
                case PAA: return 5;
                case PLA: return 6;
                default: return 0;
            }
        }

        public static int getSerializedSize(){ return Short.BYTES;}

        public static CompressionType findByShortName(String name){
            if (name == null) {
                return UNCOMPRESSED;
            }
            switch (name.trim().toUpperCase()) {
                case "UNCOMPRESSED":
                    return UNCOMPRESSED;
                case "SNAPPY":
                    return SNAPPY;
                case "GZIP":
                    return GZIP;
                case "LZO":
                    return LZO;
                case "SDT":
                    return SDT;
                case "PAA":
                    return PAA;
                case "PLA":
                    return PLA;
                default:
                    throw new CompressionTypeNotSupportedException(name);
            }
        }
        public String getExtension(){
            switch (this) {
                case UNCOMPRESSED: return "";
                case SNAPPY: return ".snappy";
                case GZIP: return ".gz";
                case LZO: return ".lzo";
                case SDT: return ".sdt";
                case PAA: return ".paa";
                case PLA: return ".pla";
                default: return "";
            }
        }

}
