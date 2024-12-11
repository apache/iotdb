package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped;

public final class SyntheticAddress
{
    private SyntheticAddress() {}

    public static long encodeSyntheticAddress(int sliceIndex, int sliceOffset)
    {
        return (((long) sliceIndex) << 32) | sliceOffset;
    }

    public static int decodeSliceIndex(long sliceAddress)
    {
        return ((int) (sliceAddress >> 32));
    }

    public static int decodePosition(long sliceAddress)
    {
        // low order bits contain the raw offset, so a simple cast here will suffice
        return (int) sliceAddress;
    }
}
