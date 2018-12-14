/**
 * File format description for the time-series file format
 */
namespace cpp tsfile
namespace java cn.edu.tsinghua.tsfile.format

/**
 * Types supported by TSFile.  These types are intended to be used in combination
 * with the encodings to control the on disk storage format.
 * For example INT16 is not included as a type since a good encoding of INT32
 * would handle this.
 */
enum DataType {
  BOOLEAN = 0;
  INT32 = 1;
  INT64 = 2;
  INT96 = 3;
  FLOAT = 4;
  DOUBLE = 5;
  TEXT = 6;
  FIXED_LEN_BYTE_ARRAY = 7;
  ENUMS = 8;
  BIGDECIMAL = 9;
}

enum FreqType {
  SINGLE_FREQ = 0;
  MULTI_FREQ = 1;
  IRREGULAR_FREQ = 2;
}

/**
 * Common types used by frameworks(e.g. hive, pig) using TSFile.  This helps map
 * between types in those frameworks to the base types in TSFile.  This is only
 * metadata and not needed to read or write the data.
 *
 * hold this place for future extension
 */
enum ConvertedType {
  /** a BYTE_ARRAY actually contains UTF8 encoded chars */
  UTF8 = 0;
}

/**
 * Digest/statistics per row group and per page
 * All fields are optional.
 */
struct Digest {
   /** Statistics of timeseries, eg. min and max value, sum value */
   1: optional map<string, binary> statistics;
   /** count of null value in the timeseries */
   2: optional i64 null_count;
   /** count of distinct values occurring */
   3: optional i64 distinct_count;
}


/**
 * Encodings supported by TSFile.  Not all encodings are valid for all types.
 */
enum Encoding {
  /** Default encoding.
   * BOOLEAN - 1 bit per value. 0 is false; 1 is true.
   * INT32 - 4 bytes per value.  Stored as little-endian.
   * INT64 - 8 bytes per value.  Stored as little-endian.
   * FLOAT - 4 bytes per value.  IEEE. Stored as little-endian.
   * DOUBLE - 8 bytes per value.  IEEE. Stored as little-endian.
   * BYTE_ARRAY - 4 byte length stored as little endian, followed by bytes.
   * FIXED_LEN_BYTE_ARRAY - Just the bytes.
   * ENUMS - 1 byte per value. Stored as little-endian.
   */
  PLAIN = 0;

  /**
   * Deprecated: Dictionary encoding. The values in the dictionary are encoded in the
   * plain type.
   * in a data page use RLE_DICTIONARY instead.
   * in a Dictionary page use PLAIN instead
   */
  PLAIN_DICTIONARY = 2;

  /** Group packed run length encoding. Usable for Booleans
   * (on one bit: 0 is false; 1 is true.)
   */
  RLE = 3;

  /** Delta encoding for integers. This can be used for series of int values and works best
   * on sorted data
   */
  DELTA_BINARY_PACKED = 5;

  /** Encoding for byte arrays to separate the length values and the data. The lengths
   * are encoded using DELTA_BINARY_PACKED
   */
  DELTA_LENGTH_BYTE_ARRAY = 6;

  /** Incremental-encoded byte array. Prefix lengths are encoded using DELTA_BINARY_PACKED.
   * Suffixes are stored as delta length byte arrays.
   */
  DELTA_BYTE_ARRAY = 7;

  /** Dictionary encoding: the ids are encoded using the RLE encoding
   */
  RLE_DICTIONARY = 8;

  /** First-order difference encoding. Usable for encoding timestamps
   *  in a time series.
   */
  DIFF = 9;

  /** Second-order difference encoding. Usable for encoding timestamps
   *  in a time series.
   */
  TS_2DIFF = 10;

  /** Bitmap encoding. Usable for encoding switch values
   *  in a time series.
   */
  BITMAP = 11;

  /**
   * Piecewise linear approximate
   */
  PLA = 12;

  /**
   * Swing door transform
   */
  SDT = 13;

  /**
   * Discrete Fourier transform
   */
  DFT = 14;
  
  /**
   * Float encoding used in Gorilla
   */
  GORILLA = 15;
}

/**
 * Supported compression algorithms.
 */
enum CompressionType {
  UNCOMPRESSED = 0;
  SNAPPY = 1;
  GZIP = 2;
  LZO = 3;
  SDT = 4;
  PAA = 5;
  PLA = 6;
}

enum PageType {
  DATA_PAGE = 0;
  INDEX_PAGE = 1;
  DICTIONARY_PAGE = 2;
}

/** Data page header, with allowing reading information without decompressing the data
 **/
struct DataPageHeader {
  /** Number of values, including NULLs, in this data page. **/
  1: required i32 num_values;


  /** Number of rows in this data page **/
  2: required i32 num_rows;

  /** Encoding used for this data page **/
  3: required Encoding encoding;

  /** Optional digest/statistics for the data in this page**/
  4: optional Digest digest;

  /**  whether the values are compressed.
  Which means the section of the page is compressed with the compression_type.
  If missing it is considered compressed */
  5: optional bool is_compressed = 1;

  6: required i64 max_timestamp;

  7: required i64 min_timestamp;
}

struct IndexPageHeader {
  /** TODO: **/
}

struct DictionaryPageHeader {
  /** Number of values in the dictionary **/
  1: required i32 num_values;

  /** Encoding using this dictionary page **/
  2: required Encoding encoding;

  /** If true, the entries in the dictionary are sorted in ascending order **/
  3: optional bool is_sorted;
}

struct PageHeader {
  /** the type of the page: indicates which of the *_header fields is set **/
  1: required PageType type;

  /** Uncompressed page size in bytes (not including this header) **/
  2: required i32 uncompressed_page_size;

  /** Compressed page size in bytes (not including this header) **/
  3: required i32 compressed_page_size;

  /** 32bit crc for the data below. This allows for disabling checksumming in HDFS
   *  if only a few pages needs to be read
   **/
  4: optional i32 crc;

  // Headers for page specific data.  One only will be set.
  5: optional DataPageHeader data_page_header;
  6: optional IndexPageHeader index_page_header;
  7: optional DictionaryPageHeader dictionary_page_header;
}

struct TimeInTimeSeriesChunkMetaData {
  1: required DataType data_type;

  2: optional FreqType freq_type;

  3: optional list<i32> frequencies;

  4: required i64 startime;

  5: required i64 endtime;

  /** If values of data consist of enum values, metadata will store all possible
   * values in time series
   */
  6: optional list<string> enum_values;

}

struct ValueInTimeSeriesChunkMetaData {
  1: required DataType data_type;

  2: optional i32 max_error;

  3: optional Digest digest;

  /** If values of data consist of enum values, metadata will store all possible
   * values in time series
   */
  4: optional list<string> enum_values;

}

enum TimeSeriesChunkType {
  TIME = 0;
  VALUE = 1;
}

/**
 * Description for time series chunk metadata
 */
struct TimeSeriesChunkMetaData {
  1: required string measurement_uid;

  /** Referenced field id in the TSFile schema **/
  2: optional i32 ref_field_id;

  /** Type of this time series **/
  3: required TimeSeriesChunkType timeseries_chunk_type;

  /** Set of all encodings used for this time series. The purpose is to validate
   * whether we can decode those pages. **/
  4: optional list<Encoding> encodings;

  /** Byte offset in file_path to the RowGroupMetaData **/
  5: required i64 file_offset;

  6: required CompressionType compression_type;

  7: optional i64 num_rows;

  /** total byte size of all uncompressed pages in this time series chunk (including the headers) **/
  8: optional i64 total_byte_size;

  /** Optional json metadata **/
  9: optional list<string> json_metadata;

  /** Byte offset from beginning of file to first data page **/
  10: optional i64 data_page_offset;

  /** Byte offset from beginning of file to root index page **/
  11: optional i64 index_page_offset;

  /** Byte offset from the beginning of file to first (only) dictionary page **/
  12: optional i64 dictionary_page_offset;

  /** optional digest/statistics for this timeseries chunk */
  13: optional Digest digest;

  14: optional TimeInTimeSeriesChunkMetaData time_tsc;

  15: optional ValueInTimeSeriesChunkMetaData value_tsc;
}

struct RowGroupMetaData {
  1: required list<TimeSeriesChunkMetaData> tsc_metadata;

  2: required string delta_object_id;

  /** Total byte size of all the uncompressed time series data in this row group **/
  3: required i64 total_byte_size;

  /** Maximum number of rows in this row group **/
  4: required i64 max_num_rows;

  /** This path is relative to the current file. **/
  5: optional string file_path;

  6: required string delta_object_type;

  7: required i64 writtenTime;
}

struct RowGroupBlockMetaData {
  1: required list<RowGroupMetaData> row_groups_metadata;

  2: optional string delta_object_id;
}

/**
 * Description for a delta object
 */
struct DeltaObject {
  /** start position of RowGroupMetadataBlock in file **/
  1: required i64 offset;

  /** size of RowGroupMetadataBlock in byte **/
  2: required i32 metadata_block_size;

  /** start time **/
  3: required i64 start_time;
  
  /** end time **/
  4: required i64 end_time;
}

/**
 * Schema definition of a time-series. Logically, a time-series could be
 * regarded as a list of timestamp-value pairs.
 */
struct TimeSeries {
  1: required string measurement_uid;

  /** Data type for this time series. */
  2: required DataType type;

  /** If type is FIXED_LEN_BYTE_ARRAY, this is the byte length of the values.
   * Otherwise, if specified, this is the maximum bit length to store any of the values.
   * (e.g. a low cardinality INT timeseries could have this set to 32).  Note that this is
   * in the schema, and therefore fixed for the entire file.
   */
  3: optional i32 type_length;

  /** Frequency type of the measurement that generates this time series data.
   * This field is reserved for optimization storage and access.
   * Currently, it is not used.
   */
  4: optional FreqType freq_type;

  /** Frequency values of the measurement that generates this time series data.
   * Note that a measurement may have multiple frequency values.
   */
  5: optional list<i32> frequencies;

  /** When the schema is the result of a conversion from another model,
   * converted_type is used to record the original type to help with cross conversion.
   */
  6: optional ConvertedType converted_type;

  /** Used when this timeseries contains decimal data.
   * See the DECIMAL converted type for more details.
   */
  7: optional i32 scale;
  8: optional i32 precision;

  /** When the original schema supports field ids, this will save the
   * original field id in the TSFile schema
   */
  9: optional i32 field_id;

  /** If values for data consist of enum values, metadata will store all possible
   * values in time series
   */
  10: optional list<string> enum_values;

  11: required string delta_object_type;

}

/**
 * Description for file metadata
 */
struct FileMetaData {
  /** Version of this file **/
  1: required i32 version;

  /** Map stores all delta object name and their info **/
  2: required map<string, DeltaObject> delta_object_map;
  
  /** TsFile schema for this file.  This schema contains metadata for all the time series. The schema is represented as a list. **/
  3: required list<TimeSeries> timeseries_list;

  /** Optional json metadata **/
  4: optional list<string> json_metadata;

  /** String for application that wrote this file.  This should be in the format
   * <Application> version <App Version> (build <App Build Hash>).
   * e.g. tsfile version 1.0 (build SHA-1_hash_code)
   **/
  5: optional string created_by;
 
  /**
   * User specified properties *
  */
  6: optional map<string, string> properties;
}