/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.tsfile.common.conf;

/**
 * TSFileConfig is a configure class. Every variables is public and has default value.
 *
 * @author kangrong
 */
public class TSFileConfig {
    // Memory configuration
    /**
     * Memory size threshold for flushing to disk or HDFS, default value is 128MB
     */
    public int groupSizeInByte = 128 * 1024 * 1024;
    /**
     * The memory size for each series writer to pack page, default value is 64KB
     */
    public int pageSizeInByte = 64 * 1024;
    /**
     * The maximum number of data points in a page, default value is 1024 * 1024
     */
    public int maxNumberOfPointsInPage = 1024 * 1024;

    // Data type configuration
    /**
     * Data type for input timestamp, TsFile supports INT32 or INT64
     */
    public String timeSeriesDataType = "INT64";
    /**
     * Max length limitation of input string
     */
    public int maxStringLength = 128;
    /**
     * Floating-point precision
     */
    public int floatPrecision = 2;

    // Encoder configuration
    /**
     * Encoder of time series, TsFile supports TS_2DIFF, PLAIN and RLE(run-length encoding) Default value is TS_2DIFF
     */
    public String timeSeriesEncoder = "TS_2DIFF";
    /**
     * Encoder of value series. default value is PLAIN. For int, long data type, TsFile also supports TS_2DIFF and
     * RLE(run-length encoding). For float, double data type, TsFile also supports TS_2DIFF, RLE(run-length encoding)
     * and GORILLA. For text data type, TsFile only supports PLAIN.
     */
    public String valueEncoder = "PLAIN";

    // RLE configuration
    /**
     * Default bit width of RLE encoding is 8
     */
    public int rleBitWidth = 8;
    public final int RLE_MIN_REPEATED_NUM = 8;
    public final int RLE_MAX_REPEATED_NUM = 0x7FFF;
    public final int RLE_MAX_BIT_PACKED_NUM = 63;

    // Gorilla encoding configuration
    public final static int FLOAT_LENGTH = 32;
    public final static int FLAOT_LEADING_ZERO_LENGTH = 5;
    public final static int FLOAT_VALUE_LENGTH = 6;
    public final static int DOUBLE_LENGTH = 64;
    public final static int DOUBLE_LEADING_ZERO_LENGTH = 6;
    public final static int DOUBLE_VALUE_LENGTH = 7;

    // TS_2DIFF configuration
    /**
     * Default block size of two-diff. delta encoding is 128
     */
    public int deltaBlockSize = 128;

    // Bitmap configuration
    public final int BITMAP_BITWIDTH = 1;

    // Freq encoder configuration
    /**
     * Default frequency type is SINGLE_FREQ
     */
    public String freqType = "SINGLE_FREQ";
    /**
     * Default PLA max error is 100
     */
    public double plaMaxError = 100;
    /**
     * Default SDT max error is 100
     */
    public double sdtMaxError = 100;
    /**
     * Default DFT satisfy rate is 0.1
     */
    public double dftSatisfyRate = 0.1;

    // Compression configuration
    /**
     * Data compression method, TsFile supports UNCOMPRESSED or SNAPPY. Default value is UNCOMPRESSED which means no
     * compression
     *
     */
    public String compressor = "UNCOMPRESSED";

    // Don't change the following configuration

    /**
     * Line count threshold for checking page memory occupied size
     */
    public int pageCheckSizeThreshold = 100;

    /**
     * Current version is 3
     */
    public static int currentVersion = 3;

    /**
     * Default endian value is LITTLE_ENDIAN
     */
    public String endian = "LITTLE_ENDIAN";

    /**
     * String encoder with UTF-8 encodes a character to at most 4 bytes.
     */
    public static final int BYTE_SIZE_PER_CHAR = 4;

    public static final String STRING_ENCODING = "UTF-8";

    public static final String CONFIG_FILE_NAME = "tsfile-format.properties";

    /**
     * The default grow size of class BatchData
     */
    public static int dynamicDataSize = 1000;

    public static final String MAGIC_STRING = "TsFilev0.8.0";

    /**
     * only can be used by TsFileDescriptor
     */
    protected TSFileConfig() {

    }
}
