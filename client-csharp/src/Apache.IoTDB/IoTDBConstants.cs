namespace Apache.IoTDB
{
    public enum TSDataType
    {
        BOOLEAN,
        INT32,
        INT64,
        FLOAT,
        DOUBLE,
        TEXT,
        
        // default value must be 0
        NONE
    }

    public enum TSEncoding
    {
        PLAIN,
        PLAIN_DICTIONARY,
        RLE,
        DIFF,
        TS_2DIFF,
        BITMAP,
        GORILLA_V1,
        REGULAR,
        GORILLA,
        
        // default value must be 0
        NONE
    }

    public enum Compressor
    {
        UNCOMPRESSED,
        SNAPPY,
        GZIP,
        LZO,
        SDT,
        PAA,
        PLA,
        LZ4
    }
}