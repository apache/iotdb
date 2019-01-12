package org.apache.iotdb.tsfile.encoding.common;

/**
 * This class defines several constants using in encoding algorithm.
 *
 * @author xuyi
 */
public class EncodingConfig {
    // if number n repeats more than(>=) RLE_MAX_REPEATED_NUM times, use rle encoding, otherwise use bit-packing
    public static final int RLE_MAX_REPEATED_NUM = 8;

    // when to start a new bit-pacing group
    public static final int RLE_MAX_BIT_PACKED_NUM = 63;

    // bit width for Bitmap Encoding
    public static final int BITMAP_BITWIDTH = 1;
}
