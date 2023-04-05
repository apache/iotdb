package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class EncodeTestTime {

    public static void main(@org.jetbrains.annotations.NotNull String[] args) throws IOException, ParseException {
        ArrayList<String> input_path_list = new ArrayList<>();
        ArrayList<String> output_path_list = new ArrayList<>();
        input_path_list.add("C:\\Users\\xiaoj\\Desktop\\Load_sensors_for_wind_power_equipment\\1");
        output_path_list.add("C:\\Users\\xiaoj\\Desktop\\Load_sensors_for_wind_power_equipment\\test_time.csv");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");


        for (int file_i = 0; file_i < input_path_list.size(); file_i++) {
            String inputPath = input_path_list.get(file_i);
            String Output = output_path_list.get(file_i);

            // speed

            File file = new File(inputPath);
            File[] tempList = file.listFiles();

            // select encoding algorithms
            TSEncoding[] encodingList = {
                    TSEncoding.PLAIN,
                    TSEncoding.TS_2DIFF,
                    TSEncoding.CHIMP,
                    TSEncoding.GORILLA,
                    TSEncoding.RLE,
                    TSEncoding.SPRINTZ,
                    TSEncoding.RLBE
//            TSEncoding.RAKE
            };
            // select compression algorithms
            CompressionType[] compressList = {
                    CompressionType.UNCOMPRESSED,
                    CompressionType.LZ4,
                    CompressionType.GZIP,
                    CompressionType.SNAPPY,
                    CompressionType.LZMA2
            };
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Column Index",
                    "Encoding Algorithm",
                    "Compress Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Compress Time",
                    "Uncompress Time",
                    "Compressed Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file

            assert tempList != null;
            int fileRepeat = 0;
            ArrayList<Integer> columnIndexes = new ArrayList<>(); // set the column indexes of compressed
            for (int i = 0; i < 1; i++) {
                columnIndexes.add(i);
            }
            for (File f : tempList) {
                System.out.println(f);
                fileRepeat += 1;
                InputStream inputStream = Files.newInputStream(f.toPath());

                ArrayList<ArrayList<String>> data = new ArrayList<>();

                CsvReader loader = new CsvReader(inputStream, '\t', StandardCharsets.UTF_8);
                // add a column to "data"

                while (loader.readRecord()) {
                    ArrayList<String> data_row = new ArrayList<>();
                    for (int index : columnIndexes) {
//                        System.out.println(loader.getValues()[index]);
                        data_row.add(loader.getValues()[index]);
                    }
                    data.add(data_row);
                }
                inputStream.close();
                loader.close();

                TSDataType dataType = TSDataType.INT64;
                for (int index : columnIndexes) {
                    // Iterate over each encoding algorithm
                    for (TSEncoding encoding : encodingList) {
                        Encoder encoder = TSEncodingBuilder.getEncodingBuilder(encoding).getEncoder(dataType);
                        Decoder decoder = Decoder.getDecoderByType(encoding, dataType);
                        long encodeTime = 0;
                        long decodeTime = 0;

                        // Iterate over each compression algorithm
                        for (CompressionType comp : compressList) {
                            ICompressor compressor = ICompressor.getCompressor(comp);
                            IUnCompressor unCompressor = IUnCompressor.getUnCompressor(comp);
                            long compressTime = 0;
                            long uncompressTime = 0;
                            double ratio = 0;
                            double compressed_size = 0;
                            ByteArrayOutputStream buffer = new ByteArrayOutputStream();

                            // test encode time
                            long s = System.nanoTime();
                            int index_1 = index;
                            for (ArrayList<String> datum : data) {
                                encoder.encode(sdf.parse(datum.get(index_1).substring(1,24)).getTime(), buffer);
                            }
                            encoder.flush(buffer);
                            long e = System.nanoTime();
                            encodeTime += (e - s);

                            // test compress time
                            byte[] elems = buffer.toByteArray();
                            s = System.nanoTime();
                            byte[] compressed = compressor.compress(elems);
                            e = System.nanoTime();
                            compressTime += (e - s);

                            // test compression ratio and compressed size
                            compressed_size = compressed.length;
                            double ratioTmp = (double) (data.size() * Long.BYTES) / (double) compressed.length;
                            ratio += ratioTmp;

                            // test uncompress time
                            s = System.nanoTime();
                            byte[] x = unCompressor.uncompress(compressed);
                            e = System.nanoTime();
                            uncompressTime += (e - s);

                            // test decode time
                            ByteBuffer ebuffer = ByteBuffer.wrap(buffer.toByteArray());
                            s = System.nanoTime();
//                    int index_decoded = 0;
//                    while (decoder.hasNext(ebuffer)) {
//                      double d = decoder.readDouble(ebuffer);
//                      if(d!=tmp.get(index_decoded)){
////                        System.out.println(d);
//                        break;
//                      }
//                      index_decoded ++;
//                      if(d<0) break;
//                    }
//                    System.out.println(index_decoded);
                            e = System.nanoTime();
                            decodeTime += (e - s);

                            buffer.close();


//                                    ratio /= repeatTime;
//                                    compressed_size /= repeatTime;
//                                    encodeTime /= repeatTime;
//                                    decodeTime /= repeatTime;
//                                    compressTime /= repeatTime;
//                                    uncompressTime /= repeatTime;

                            // write info to file
                            String[] record = {
                                    f.toString(),
                                    String.valueOf(index_1),
                                    encoding.toString(),
                                    comp.toString(),
                                    String.valueOf(encodeTime),
                                    String.valueOf(decodeTime),
                                    String.valueOf(compressTime),
                                    String.valueOf(uncompressTime),
                                    String.valueOf(compressed_size),
                                    String.valueOf(ratio)
                            };
                            System.out.println(encoding);
                            System.out.println(comp);
                            System.out.println(ratio);
                            writer.writeRecord(record);

                        }
                    }

                }

//      break;
            }
            writer.close();
        }
    }
}
