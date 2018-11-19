package cn.edu.tsinghua.tsfile.file.metadata.enums;

import cn.edu.tsinghua.tsfile.common.exception.CompressionTypeNotSupportedException;
import cn.edu.tsinghua.tsfile.format.CompressionType;

public enum CompressionTypeName {
    UNCOMPRESSED(CompressionType.UNCOMPRESSED, ""),
    SNAPPY(CompressionType.SNAPPY, ".snappy"),
    GZIP(CompressionType.GZIP, ".gz"),
    LZO(CompressionType.LZO, ".lzo"),
    SDT(CompressionType.SDT, ".sdt"),
    PAA(CompressionType.PAA, ".paa"),
    PLA(CompressionType.PLA, ".pla");

    private final CompressionType tsfileCompressionType;
    private final String extension;
    private CompressionTypeName(CompressionType tsfileCompressionType, String extension) {
        this.tsfileCompressionType = tsfileCompressionType;
        this.extension = extension;
    }

    public static CompressionTypeName fromConf(String name) {
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

    public CompressionType getTsfileCompressionCodec() {
        return tsfileCompressionType;
    }

    public String getExtension() {
        return extension;
    }
}