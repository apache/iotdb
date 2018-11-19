package cn.edu.tsinghua.tsfile.encoding.common;

/**
 * this class define several constant int variables used in encoding.
 *
 * @author xuyi
 */
public class EncodingConfig {
    // if number n repeats more than 8(>= 8), use rle encoding, otherwise use bit-packing
    public static final int RLE_MAX_REPEATED_NUM = 8;

    // when to start a new bit-pacing group
    public static final int RLE_MAX_BIT_PACKED_NUM = 63;

    // bit width for Bitmap Encoding
    public static final int BITMAP_BITWIDTH = 1;
}
