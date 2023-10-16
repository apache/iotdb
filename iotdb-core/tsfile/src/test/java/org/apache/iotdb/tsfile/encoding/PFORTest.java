package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import me.lemire.integercompression.*;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;

import static org.apache.iotdb.tsfile.encoding.myPFOR.compressOneBlockOpt;
import static org.apache.iotdb.tsfile.encoding.myPFOR.decompressOneBlock;

public class PFORTest {

    public static int[] getAbsDeltaTsBlock(
            int[] ts_block,
            ArrayList<Integer> min_delta) {
        int[] ts_block_delta = new int[ts_block.length - 1];


        int value_delta_min = Integer.MAX_VALUE;
        for (int i = 1; i < ts_block.length; i++) {

            int epsilon_v = ts_block[i] - ts_block[i - 1];

            if (epsilon_v < value_delta_min) {
                value_delta_min = epsilon_v;
            }

        }
        min_delta.add(ts_block[0]);
        min_delta.add(value_delta_min);

        for (int i = 1; i < ts_block.length; i++) {
            int epsilon_v = ts_block[i] - value_delta_min - ts_block[i - 1];
            ts_block_delta[i - 1] = epsilon_v;
        }
        return ts_block_delta;
    }

    public static int zigzag(int num) {
        if (num < 0) return ((-num) << 1) - 1;
        else return num << 1;
    }

    public static int deZigzag(int num) {
        if (num % 2 == 0) return num >> 1;
        else return -((num + 1) >> 1);
    }

    public static int[] getAbsDeltaTsBlockSPRINTZ(
            int[] ts_block,
            ArrayList<Integer> min_delta) {
        int[] ts_block_delta = new int[ts_block.length - 1];

        for (int i = 1; i < ts_block.length; i++) {

            int epsilon_v = ts_block[i] - ts_block[i - 1];
            epsilon_v = zigzag(epsilon_v);
            ts_block_delta[i - 1] = epsilon_v;

        }
        min_delta.add(ts_block[0]);

        return ts_block_delta;
    }

    public static ArrayList<Integer> getAbsDeltaTsBlockRLE(
            int[] ts_block,
            ArrayList<Integer> min_delta,
            ArrayList<Integer> repeat_count) {
        ArrayList<Integer> ts_block_delta = new ArrayList<>();

        int value_delta_min = Integer.MAX_VALUE;
        for (Integer integer : ts_block) {
            if (integer < value_delta_min) value_delta_min = integer;
        }
        int repeat_i = 0;
        int pre_delta = ts_block[0] - value_delta_min;
        int pre_count = 1;
        min_delta.add(value_delta_min);
        int block_size = ts_block.length;

        for (int i = 1; i < block_size; i++) {
            int delta = ts_block[i] - value_delta_min;
            if (delta == pre_delta) {
                pre_count++;
            } else {
                if (pre_count > 7) {
                    repeat_count.add(repeat_i); // index_repeat
                    repeat_count.add(pre_count); // repeat_count
                    ts_block_delta.add(pre_delta);
                } else {
                    for (int j = 0; j < pre_count; j++)
                        ts_block_delta.add(pre_delta);
                }
                pre_count = 1;
                repeat_i = i;
            }
            pre_delta = delta;
        }
        for (int j = 0; j < pre_count; j++)
            ts_block_delta.add(pre_delta);
        return ts_block_delta;
    }


    public static void main(@NotNull String[] args) throws IOException {
        String parent_dir = "iotdb/iotdb-core/tsfile/src/test/resources/"; // your data path
        String output_parent_dir = parent_dir + "pfor_ratio/";
        String input_parent_dir = parent_dir + "trans_data/";
        ArrayList<String> input_path_list = new ArrayList<>();
        ArrayList<String> output_path_list = new ArrayList<>();
        ArrayList<String> dataset_name = new ArrayList<>();
        ArrayList<String> encoding_list = new ArrayList<>();
        ArrayList<String> encoding_list0 = new ArrayList<>();
        int blocksize = 2048;

        dataset_name.add("CS-Sensors");
        dataset_name.add("Metro-Traffic");
        dataset_name.add("USGS-Earthquakes");
        dataset_name.add("YZ-Electricity");
        dataset_name.add("GW-Magnetic");
        dataset_name.add("TY-Fuel");
        dataset_name.add("Cyber-Vehicle");
        dataset_name.add("Vehicle-Charge");
        dataset_name.add("Nifty-Stocks");
        dataset_name.add("TH-Climate");
        dataset_name.add("TY-Transport");
        dataset_name.add("EPM-Education");

        encoding_list0.add("TS_2DIFF");
        encoding_list0.add("RLE");
        encoding_list0.add("SPRINTZ");

        encoding_list.add("NEWPFOR");
        encoding_list.add("OPTPFOR");
        encoding_list.add("FASTPFOR");
        encoding_list.add("PFOR");

        for (String item : dataset_name) {
            input_path_list.add(input_parent_dir + item);
        }

        output_path_list.add(output_parent_dir + "CS-Sensors_ratio.csv"); // 0

        output_path_list.add(output_parent_dir + "Metro-Traffic_ratio.csv");// 1

        output_path_list.add(output_parent_dir + "USGS-Earthquakes_ratio.csv");// 2

        output_path_list.add(output_parent_dir + "YZ-Electricity_ratio.csv"); // 3

        output_path_list.add(output_parent_dir + "GW-Magnetic_ratio.csv"); //4

        output_path_list.add(output_parent_dir + "TY-Fuel_ratio.csv");//5

        output_path_list.add(output_parent_dir + "Cyber-Vehicle_ratio.csv"); //6

        output_path_list.add(output_parent_dir + "Vehicle-Charge_ratio.csv");//7

        output_path_list.add(output_parent_dir + "Nifty-Stocks_ratio.csv");//8

        output_path_list.add(output_parent_dir + "TH-Climate_ratio.csv");//9

        output_path_list.add(output_parent_dir + "TY-Transport_ratio.csv");//10

        output_path_list.add(output_parent_dir + "EPM-Education_ratio.csv");//11


        for (int file_i = 0; file_i < input_path_list.size(); file_i++) {
            String inputPath = input_path_list.get(file_i);
            String Output = output_path_list.get(file_i);

            File file = new File(inputPath);
            File[] tempList = file.listFiles();

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
                    "Points",
                    "Compressed Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file

            assert tempList != null;
            int fileRepeat;
            ArrayList<Integer> columnIndexes = new ArrayList<>(); // set the column indexes of compressed
            for (int i = 0; i < 2; i++) {
                columnIndexes.add(i, i);
            }
            int points = 0;
            IntegerCODEC codec = new Composition(new JustCopy(), new VariableByte());
            IntWrapper inputoffset;
            IntWrapper outputoffset;
            int[] compressed;
            for (String encoding : encoding_list) {
                if (Objects.equals(encoding, "FASTPFOR")) {
                    codec = new Composition(new FastPFOR(), new VariableByte());
                } else if (Objects.equals(encoding, "NEWPFOR")) {
                    codec = new Composition(new NewPFDS16(), new VariableByte());
                } else if (Objects.equals(encoding, "OPTPFOR")) {
                    codec = new Composition(new OptPFDS16(), new VariableByte());
                } else if (Objects.equals(encoding, "PFOR")) {
                    for (File f : tempList) {
                        for (String encoding0 : encoding_list0) {
                            fileRepeat = 0;
                            double final_ratio = 0;
                            int final_compressed_size = 0;
                            System.out.println(f);
                            long encodeTime = 0;
                            long decodeTime = 0;
                            while (fileRepeat < 1) {
                                fileRepeat += 1;
                                InputStream inputStream = Files.newInputStream(f.toPath());
                                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                                ArrayList<String> data = new ArrayList<>();
                                for (int index : columnIndexes) {
                                    if (index == 0) {
                                        continue;
                                    }
                                    loader.readHeaders();
                                    data.clear();
                                    while (loader.readRecord()) {
                                        String v = loader.getValues()[index];
                                        data.add(v);
                                    }
                                    points = data.size();
                                    inputStream.close();

                                    int[] origin = new int[data.size()];
                                    int i = 0;
                                    for (String value : data) {
                                        origin[i] = Integer.parseInt(value);
                                        i++;
                                    }
                                    switch (encoding0) {
                                        case "TS_2DIFF": {
                                            //TS_2DIFF+PFOR
                                            ArrayList<Integer> min = new ArrayList<>();
                                            ArrayList<Integer> outBlock = new ArrayList<>();
                                            long s = System.nanoTime();
                                            int offset = 0;
                                            while (offset < data.size()) {
                                                int endindex = offset + blocksize;
                                                if (endindex >= data.size()) {
                                                    endindex = data.size();
                                                }
                                                int[] slice = Arrays.copyOfRange(origin, offset, endindex);
                                                int[] new_slice = getAbsDeltaTsBlock(slice, min);
                                                int[] sliceoutBlock = compressOneBlockOpt(new_slice, new_slice.length);
                                                outBlock.add(sliceoutBlock.length);
                                                outBlock.add(endindex - offset - 1);
                                                for (i = 0; i < sliceoutBlock.length; i++) {
                                                    outBlock.add(sliceoutBlock[i]);
                                                }
                                                offset = endindex;
                                            }
                                            int[] minList = new int[min.size()];
                                            i = 0;
                                            for (int value : min) {
                                                minList[i] = value;
                                                i++;
                                            }
                                            int[] sliceoutBlock = compressOneBlockOpt(minList, minList.length);
                                            for (i = sliceoutBlock.length - 1; i >= 0; i--) {
                                                outBlock.add(0, sliceoutBlock[i]);
                                            }
                                            outBlock.add(0, minList.length);
                                            outBlock.add(0, sliceoutBlock.length);
                                            int[] outBlock2 = new int[outBlock.size()];
                                            i = 0;
                                            for (int value : outBlock) {
                                                outBlock2[i] = value;
                                                i++;
                                            }
                                            encodeTime += System.nanoTime() - s;
                                            s = System.nanoTime();
                                            offset = 0;
                                            int[] uncompressed = new int[data.size()];
                                            int j = 0;
                                            int k = 0;
                                            int first_slice_size = outBlock.get(offset);
                                            int first_endindex = offset + 2 + first_slice_size;
                                            int min_origin_size = outBlock.get(offset + 1);
                                            int[] new_min = new int[min_origin_size];
                                            int[] min_sliceBlock = Arrays.copyOfRange(outBlock2, offset + 2, first_endindex);
                                            decompressOneBlock(new_min, min_sliceBlock, min_origin_size);
                                            offset = first_endindex;
                                            while (offset != outBlock.size()) {
                                                int slice_size = outBlock.get(offset);
                                                int endindex = offset + 2 + slice_size;
                                                int origin_size = outBlock.get(offset + 1);
                                                int[] sliceUncompressed = new int[origin_size];
                                                int[] sliceBlock = Arrays.copyOfRange(outBlock2, offset + 2, endindex);
                                                decompressOneBlock(sliceUncompressed, sliceBlock, origin_size);
                                                uncompressed[j] = new_min[k];
                                                j++;
                                                k++;
                                                for (i = 0; i < origin_size; i++) {
                                                    uncompressed[j] = sliceUncompressed[i] + new_min[k] + uncompressed[j - 1];
                                                    j++;
                                                }
                                                k++;
                                                offset = endindex;
                                            }
                                            decodeTime += System.nanoTime() - s;
                                            final_ratio += 1.0 * outBlock.size() / origin.length;
                                            final_compressed_size += outBlock.size() * 4;
                                            break;
                                        }
                                        case "SPRINTZ": {
                                            //TS_2DIFF+SPRINTZ
                                            ArrayList<Integer> min = new ArrayList<>();
                                            ArrayList<Integer> outBlock = new ArrayList<>();
                                            long s = System.nanoTime();
                                            int offset = 0;
                                            while (offset < data.size()) {
                                                int endindex = offset + blocksize;
                                                if (endindex >= data.size()) {
                                                    endindex = data.size();
                                                }
                                                int[] slice = Arrays.copyOfRange(origin, offset, endindex);
                                                int[] new_slice = getAbsDeltaTsBlockSPRINTZ(slice, min);
                                                int[] sliceoutBlock = compressOneBlockOpt(new_slice, new_slice.length);
                                                outBlock.add(sliceoutBlock.length);
                                                outBlock.add(endindex - offset - 1);
                                                for (i = 0; i < sliceoutBlock.length; i++) {
                                                    outBlock.add(sliceoutBlock[i]);
                                                }
                                                offset = endindex;
                                            }
                                            int[] minList = new int[min.size()];
                                            i = 0;
                                            for (int value : min) {
                                                minList[i] = value;
                                                i++;
                                            }
                                            int[] sliceoutBlock = compressOneBlockOpt(minList, minList.length);
                                            for (i = sliceoutBlock.length - 1; i >= 0; i--) {
                                                outBlock.add(0, sliceoutBlock[i]);
                                            }
                                            outBlock.add(0, minList.length);
                                            outBlock.add(0, sliceoutBlock.length);
                                            int[] outBlock2 = new int[outBlock.size()];
                                            i = 0;
                                            for (int value : outBlock) {
                                                outBlock2[i] = value;
                                                i++;
                                            }
                                            encodeTime += System.nanoTime() - s;
                                            s = System.nanoTime();
                                            offset = 0;
                                            int[] uncompressed = new int[data.size()];
                                            int j = 0;
                                            int k = 0;
                                            int first_slice_size = outBlock.get(offset);
                                            int first_endindex = offset + 2 + first_slice_size;
                                            int min_origin_size = outBlock.get(offset + 1);
                                            int[] new_min = new int[min_origin_size];
                                            int[] min_sliceBlock = Arrays.copyOfRange(outBlock2, offset + 2, first_endindex);
                                            decompressOneBlock(new_min, min_sliceBlock, min_origin_size);
                                            offset = first_endindex;
                                            while (offset != outBlock.size()) {
                                                int slice_size = outBlock.get(offset);
                                                int endindex = offset + 2 + slice_size;
                                                int origin_size = outBlock.get(offset + 1);
                                                int[] sliceUncompressed = new int[origin_size];
                                                int[] sliceBlock = Arrays.copyOfRange(outBlock2, offset + 2, endindex);
                                                decompressOneBlock(sliceUncompressed, sliceBlock, origin_size);

                                                uncompressed[j] = new_min[k];
                                                j++;
                                                k++;
                                                for (i = 0; i < origin_size; i++) {
                                                    uncompressed[j] = deZigzag(sliceUncompressed[i]) + uncompressed[j - 1];
                                                    j++;
                                                }
                                                offset = endindex;
                                            }
                                            decodeTime += System.nanoTime() - s;
                                            final_ratio += 1.0 * outBlock.size() / origin.length;
                                            final_compressed_size += outBlock.size() * 4;
                                            break;
                                        }
                                        case "RLE": {
                                            //TS_2DIFF+RLE+++
                                            ArrayList<Integer> min = new ArrayList<>();
                                            ArrayList<Integer> outBlock = new ArrayList<>();
                                            long s = System.nanoTime();
                                            int offset = 0;
                                            while (offset < data.size()) {
                                                int endindex = offset + blocksize;
                                                if (endindex >= data.size()) {
                                                    endindex = data.size();
                                                }
                                                int[] slice = Arrays.copyOfRange(origin, offset, endindex);
                                                ArrayList<Integer> repeat_list = new ArrayList<>();
                                                ArrayList<Integer> new_slice_list = getAbsDeltaTsBlockRLE(slice, min, repeat_list);
                                                int[] new_slice = new int[new_slice_list.size()];
                                                i = 0;
                                                for (int value : new_slice_list) {
                                                    new_slice[i++] = value;
                                                }
                                                int[] repeat = new int[repeat_list.size()];
                                                i = 0;
                                                for (int value : repeat_list) {
                                                    repeat[i++] = value;
                                                }
                                                int[] sliceoutBlock = compressOneBlockOpt(new_slice, new_slice.length);
                                                outBlock.add(sliceoutBlock.length);
                                                outBlock.add(new_slice.length);
                                                for (i = 0; i < sliceoutBlock.length; i++) {
                                                    outBlock.add(sliceoutBlock[i]);
                                                }
                                                int[] sliceoutBlock2 = compressOneBlockOpt(repeat, repeat.length);
                                                outBlock.add(sliceoutBlock2.length);
                                                outBlock.add(repeat.length);
                                                for (i = 0; i < sliceoutBlock2.length; i++) {
                                                    outBlock.add(sliceoutBlock2[i]);
                                                }
                                                offset = endindex;
                                            }
                                            int[] minList = new int[min.size()];
                                            i = 0;
                                            for (int value : min) {
                                                minList[i] = value;
                                                i++;
                                            }
                                            int[] sliceoutBlock = compressOneBlockOpt(minList, minList.length);
                                            for (i = sliceoutBlock.length - 1; i >= 0; i--) {
                                                outBlock.add(0, sliceoutBlock[i]);
                                            }
                                            outBlock.add(0, minList.length);
                                            outBlock.add(0, sliceoutBlock.length);
                                            int[] outBlock2 = new int[outBlock.size()];
                                            i = 0;
                                            for (int value : outBlock) {
                                                outBlock2[i] = value;
                                                i++;
                                            }
                                            encodeTime += System.nanoTime() - s;
                                            s = System.nanoTime();
                                            offset = 0;
                                            int[] uncompressed = new int[data.size()];
                                            int j = 0;
                                            int k = 0;
                                            int first_slice_size = outBlock.get(offset);
                                            int first_endindex = offset + 2 + first_slice_size;
                                            int min_origin_size = outBlock.get(offset + 1);
                                            int[] new_min = new int[min_origin_size];
                                            int[] min_sliceBlock = Arrays.copyOfRange(outBlock2, offset + 2, first_endindex);
                                            decompressOneBlock(new_min, min_sliceBlock, min_origin_size);
                                            offset = first_endindex;
                                            while (offset != outBlock.size()) {
                                                //RLE---
                                                int slice_size = outBlock.get(offset);
                                                int endindex = offset + 2 + slice_size;
                                                int origin_size = outBlock.get(offset + 1);
                                                int[] sliceUncompressed = new int[origin_size];
                                                int[] sliceBlock = Arrays.copyOfRange(outBlock2, offset + 2, endindex);
                                                decompressOneBlock(sliceUncompressed, sliceBlock, origin_size);
                                                offset = endindex;
                                                slice_size = outBlock.get(offset);
                                                endindex = offset + 2 + slice_size;
                                                int origin_size2 = outBlock.get(offset + 1);
                                                int[] sliceUncompressed2 = new int[origin_size2];//
                                                int[] sliceBlock2 = Arrays.copyOfRange(outBlock2, offset + 2, endindex);
                                                decompressOneBlock(sliceUncompressed2, sliceBlock2, origin_size2);

                                                int index2 = 0;
                                                int index1;
                                                int fake_index = 0;
                                                for (index1 = 0; index1 < origin_size; index1++) {
                                                    if (index2 < sliceUncompressed2.length && fake_index == sliceUncompressed2[index2]) {
                                                        //repeat
                                                        int times = sliceUncompressed2[index2 + 1];
                                                        for (i = 0; i < times; i++) {
                                                            uncompressed[j] = sliceUncompressed[index1] + new_min[k];
                                                            j++;
                                                        }
                                                        fake_index += sliceUncompressed2[index2 + 1];
                                                        index2 += 2;
                                                    } else {
                                                        uncompressed[j] = sliceUncompressed[index1] + new_min[k];
                                                        j++;
                                                        fake_index++;
                                                    }
                                                }
                                                offset = endindex;
                                                k++;
                                            }
                                            decodeTime += System.nanoTime() - s;
                                            final_ratio += 1.0 * outBlock.size() / origin.length;
                                            final_compressed_size += outBlock.size() * 4;
                                            break;
                                        }
                                    }
                                }
                            }
                            String[] record = {
                                    f.toString(),
                                    "1",
                                    encoding0 + "+" + encoding,
                                    "NOCOMPRESSION",
                                    String.valueOf(1.0 * encodeTime / fileRepeat),
                                    String.valueOf(1.0 * decodeTime / fileRepeat),
                                    "0",
                                    "0",
                                    String.valueOf(points),
                                    String.valueOf(final_compressed_size / fileRepeat),
                                    String.valueOf(final_ratio / fileRepeat)
                            };

                            writer.writeRecord(record);
                        }
                    }
                    continue;
                }
                for (File f : tempList) {
                    for (String encoding0 : encoding_list0) {
                        switch (encoding0) {
                            case "TS_2DIFF": {
                                fileRepeat = 0;
                                double final_ratio = 0;
                                int final_compressed_size = 0;
                                System.out.println(f);
                                long encodeTime = 0;
                                long decodeTime = 0;

                                while (fileRepeat < 1) {
                                    fileRepeat += 1;
                                    InputStream inputStream = Files.newInputStream(f.toPath());
                                    CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                                    ArrayList<String> data = new ArrayList<>();

                                    for (int index : columnIndexes) {
                                        if (index == 0) {
                                            continue;
                                        }
                                        loader.readHeaders();
                                        data.clear();
                                        while (loader.readRecord()) {
                                            String v = loader.getValues()[index];
                                            data.add(v);
                                        }
                                        points = data.size();
                                        inputStream.close();

                                        int[] origin = new int[data.size()];
                                        int i = 0;
                                        for (String value : data) {
                                            origin[i++] = Integer.parseInt(value);
                                        }
                                        // TS_2DIFF
                                        ArrayList<Integer> min = new ArrayList<>();
                                        ArrayList<Integer> outBlock = new ArrayList<>();
                                        long s = System.nanoTime();
                                        int offset = 0;
                                        while (offset < data.size()) {
                                            int endindex = offset + blocksize;
                                            if (endindex >= data.size()) {
                                                endindex = data.size();
                                            }
                                            int[] slice = Arrays.copyOfRange(origin, offset, endindex);
                                            int[] tmp = getAbsDeltaTsBlock(slice, min);
                                            compressed = new int[tmp.length + 1024];
                                            inputoffset = new IntWrapper(0);
                                            outputoffset = new IntWrapper(0);
                                            codec.compress(tmp, inputoffset, tmp.length, compressed, outputoffset);
                                            compressed = Arrays.copyOf(compressed, outputoffset.intValue());
                                            outBlock.add(compressed.length);
                                            outBlock.add(endindex - offset - 1);
                                            for (i = 0; i < compressed.length; i++) {
                                                outBlock.add(compressed[i]);
                                            }
                                            offset = endindex;
                                        }
                                        int[] minList = new int[min.size()];
                                        i = 0;
                                        for (int value : min) {
                                            minList[i] = value;
                                            i++;
                                        }
                                        compressed = new int[minList.length + 1024];
                                        inputoffset = new IntWrapper(0);
                                        outputoffset = new IntWrapper(0);
                                        codec.compress(minList, inputoffset, minList.length, compressed, outputoffset);
                                        compressed = Arrays.copyOf(compressed, outputoffset.intValue());
                                        for (i = compressed.length - 1; i >= 0; i--) {
                                            outBlock.add(0, compressed[i]);
                                        }
                                        outBlock.add(0, minList.length);
                                        outBlock.add(0, compressed.length);
                                        int[] outBlock2 = new int[outBlock.size()];
                                        i = 0;
                                        for (int value : outBlock) {
                                            outBlock2[i] = value;
                                            i++;
                                        }
                                        encodeTime += System.nanoTime() - s;
                                        s = System.nanoTime();
                                        offset = 0;
                                        int[] uncompressed = new int[data.size()];
                                        int j = 0;
                                        int k = 0;
                                        int first_slice_size = outBlock.get(offset);
                                        int first_endindex = offset + 2 + first_slice_size;
                                        int min_origin_size = outBlock.get(offset + 1);
                                        int[] new_min = new int[min_origin_size];
                                        int[] min_sliceBlock = Arrays.copyOfRange(outBlock2, offset + 2, first_endindex);
                                        IntWrapper recoffset = new IntWrapper(0);
                                        codec.uncompress(min_sliceBlock, new IntWrapper(0), min_sliceBlock.length,
                                                new_min, recoffset);
                                        offset = first_endindex;
                                        while (offset != outBlock.size()) {
                                            int slice_size = outBlock.get(offset);
                                            int endindex = offset + 2 + slice_size;
                                            int origin_size = outBlock.get(offset + 1);
                                            int[] sliceBlock = Arrays.copyOfRange(outBlock2, offset + 2, endindex);
                                            int[] recovered = new int[origin_size];
                                            recoffset = new IntWrapper(0);
                                            codec.uncompress(sliceBlock, new IntWrapper(0), sliceBlock.length,
                                                    recovered, recoffset);
                                            uncompressed[j] = new_min[k];
                                            j++;
                                            k++;
                                            for (i = 0; i < origin_size; i++) {
                                                uncompressed[j] = recovered[i] + new_min[k] + uncompressed[j - 1];
                                                j++;
                                            }
                                            k++;
                                            offset = endindex;
                                        }
                                        decodeTime += System.nanoTime() - s;


                                        final_ratio += 1.0 * outBlock.size() / origin.length;
                                        final_compressed_size += outBlock.size() * 4;
                                    }
                                }

                                String[] record = {
                                        f.toString(),
                                        "1",
                                        encoding0 + "+" + encoding,
                                        "NOCOMPRESSION",
                                        String.valueOf(1.0 * encodeTime / fileRepeat),
                                        String.valueOf(1.0 * decodeTime / fileRepeat),
                                        "0",
                                        "0",
                                        String.valueOf(points),
                                        String.valueOf(final_compressed_size / fileRepeat),
                                        String.valueOf(final_ratio / fileRepeat)
                                };
                                writer.writeRecord(record);
                                break;
                            }
                            case "SPRINTZ": {
                                fileRepeat = 0;
                                double final_ratio = 0;
                                int final_compressed_size = 0;
                                System.out.println(f);
                                long encodeTime = 0;
                                long decodeTime = 0;

                                while (fileRepeat < 1) {
                                    fileRepeat += 1;
                                    InputStream inputStream = Files.newInputStream(f.toPath());
                                    CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                                    ArrayList<String> data = new ArrayList<>();

                                    for (int index : columnIndexes) {
                                        if (index == 0) {
                                            continue;
                                        }
                                        loader.readHeaders();
                                        data.clear();
                                        while (loader.readRecord()) {
                                            String v = loader.getValues()[index];
                                            data.add(v);
                                        }
                                        points = data.size();
                                        inputStream.close();

                                        int[] origin = new int[data.size()];
                                        int i = 0;
                                        for (String value : data) {
                                            origin[i++] = Integer.parseInt(value);
                                        }
                                        // SPRINTZ
                                        ArrayList<Integer> min = new ArrayList<>();
                                        ArrayList<Integer> outBlock = new ArrayList<>();
                                        long s = System.nanoTime();
                                        int offset = 0;
                                        while (offset < data.size()) {
                                            int endindex = offset + blocksize;
                                            if (endindex >= data.size()) {
                                                endindex = data.size();
                                            }
                                            int[] slice = Arrays.copyOfRange(origin, offset, endindex);
                                            int[] tmp = getAbsDeltaTsBlockSPRINTZ(slice, min);
                                            compressed = new int[tmp.length + 1024];
                                            inputoffset = new IntWrapper(0);
                                            outputoffset = new IntWrapper(0);
                                            codec.compress(tmp, inputoffset, tmp.length, compressed, outputoffset);
                                            compressed = Arrays.copyOf(compressed, outputoffset.intValue());
                                            outBlock.add(compressed.length);
                                            outBlock.add(endindex - offset - 1);
                                            for (i = 0; i < compressed.length; i++) {
                                                outBlock.add(compressed[i]);
                                            }
                                            offset = endindex;
                                        }
                                        int[] minList = new int[min.size()];
                                        i = 0;
                                        for (int value : min) {
                                            minList[i] = value;
                                            i++;
                                        }
                                        compressed = new int[minList.length + 1024];
                                        inputoffset = new IntWrapper(0);
                                        outputoffset = new IntWrapper(0);
                                        codec.compress(minList, inputoffset, minList.length, compressed, outputoffset);
                                        compressed = Arrays.copyOf(compressed, outputoffset.intValue());
                                        for (i = compressed.length - 1; i >= 0; i--) {
                                            outBlock.add(0, compressed[i]);
                                        }
                                        outBlock.add(0, minList.length);
                                        outBlock.add(0, compressed.length);
                                        int[] outBlock2 = new int[outBlock.size()];
                                        i = 0;
                                        for (int value : outBlock) {
                                            outBlock2[i] = value;
                                            i++;
                                        }
                                        encodeTime += System.nanoTime() - s;
                                        s = System.nanoTime();
                                        offset = 0;
                                        int[] uncompressed = new int[data.size()];
                                        int j = 0;
                                        int k = 0;
                                        int first_slice_size = outBlock.get(offset);
                                        int first_endindex = offset + 2 + first_slice_size;
                                        int min_origin_size = outBlock.get(offset + 1);
                                        int[] new_min = new int[min_origin_size];
                                        int[] min_sliceBlock = Arrays.copyOfRange(outBlock2, offset + 2, first_endindex);
                                        IntWrapper recoffset = new IntWrapper(0);
                                        codec.uncompress(min_sliceBlock, new IntWrapper(0), min_sliceBlock.length,
                                                new_min, recoffset);
                                        offset = first_endindex;
                                        while (offset != outBlock.size()) {
                                            int slice_size = outBlock.get(offset);
                                            int endindex = offset + 2 + slice_size;
                                            int origin_size = outBlock.get(offset + 1);
                                            int[] sliceBlock = Arrays.copyOfRange(outBlock2, offset + 2, endindex);
                                            int[] recovered = new int[origin_size];
                                            recoffset = new IntWrapper(0);
                                            codec.uncompress(sliceBlock, new IntWrapper(0), sliceBlock.length,
                                                    recovered, recoffset);
                                            uncompressed[j] = new_min[k];
                                            j++;
                                            k++;
                                            for (i = 0; i < origin_size; i++) {
                                                uncompressed[j] = deZigzag(recovered[i]) + uncompressed[j - 1];
                                                j++;
                                            }
                                            offset = endindex;
                                        }
                                        decodeTime += System.nanoTime() - s;


                                        final_ratio += 1.0 * outBlock.size() / origin.length;
                                        final_compressed_size += outBlock.size() * 4;
                                    }
                                }

                                String[] record = {
                                        f.toString(),
                                        "1",
                                        encoding0 + "+" + encoding,
                                        "NOCOMPRESSION",
                                        String.valueOf(1.0 * encodeTime / fileRepeat),
                                        String.valueOf(1.0 * decodeTime / fileRepeat),
                                        "0",
                                        "0",
                                        String.valueOf(points),
                                        String.valueOf(final_compressed_size / fileRepeat),
                                        String.valueOf(final_ratio / fileRepeat)
                                };
                                writer.writeRecord(record);
                                break;
                            }
                            case "RLE": {
                                fileRepeat = 0;
                                double final_ratio = 0;
                                int final_compressed_size = 0;
                                System.out.println(f);
                                long encodeTime = 0;
                                long decodeTime = 0;

                                while (fileRepeat < 1) {
                                    fileRepeat += 1;
                                    InputStream inputStream = Files.newInputStream(f.toPath());
                                    CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                                    ArrayList<String> data = new ArrayList<>();

                                    for (int index : columnIndexes) {
                                        if (index == 0) {
                                            continue;
                                        }
                                        loader.readHeaders();
                                        data.clear();
                                        while (loader.readRecord()) {
                                            String v = loader.getValues()[index];
                                            data.add(v);
                                        }
                                        points = data.size();
                                        inputStream.close();

                                        int[] origin = new int[data.size()];
                                        int i = 0;
                                        for (String value : data) {
                                            origin[i++] = Integer.parseInt(value);
                                        }
                                        // RLE+++
                                        ArrayList<Integer> min = new ArrayList<>();
                                        ArrayList<Integer> outBlock = new ArrayList<>();
                                        long s = System.nanoTime();
                                        int offset = 0;
                                        while (offset < data.size()) {
                                            int endindex = offset + blocksize;
                                            if (endindex >= data.size()) {
                                                endindex = data.size();
                                            }
                                            int[] slice = Arrays.copyOfRange(origin, offset, endindex);
                                            ArrayList<Integer> repeat_list = new ArrayList<>();
                                            ArrayList<Integer> new_slice_list = getAbsDeltaTsBlockRLE(slice, min, repeat_list);
                                            int[] tmp = new int[new_slice_list.size()];
                                            i = 0;
                                            for (int value : new_slice_list) {
                                                tmp[i++] = value;
                                            }
                                            int[] repeat = new int[repeat_list.size()];
                                            i = 0;
                                            for (int value : repeat_list) {
                                                repeat[i++] = value;
                                            }
                                            compressed = new int[tmp.length + 1024];
                                            inputoffset = new IntWrapper(0);
                                            outputoffset = new IntWrapper(0);
                                            codec.compress(tmp, inputoffset, tmp.length, compressed, outputoffset);
                                            compressed = Arrays.copyOf(compressed, outputoffset.intValue());
                                            outBlock.add(compressed.length);
                                            outBlock.add(tmp.length);
                                            for (i = 0; i < compressed.length; i++) {
                                                outBlock.add(compressed[i]);
                                            }
                                            compressed = new int[repeat.length + 1024];
                                            inputoffset = new IntWrapper(0);
                                            outputoffset = new IntWrapper(0);
                                            codec.compress(repeat, inputoffset, repeat.length, compressed, outputoffset);
                                            compressed = Arrays.copyOf(compressed, outputoffset.intValue());
                                            outBlock.add(compressed.length);
                                            outBlock.add(repeat.length);
                                            for (i = 0; i < compressed.length; i++) {
                                                outBlock.add(compressed[i]);
                                            }
                                            offset = endindex;
                                        }
                                        int[] minList = new int[min.size()];
                                        i = 0;
                                        for (int value : min) {
                                            minList[i] = value;
                                            i++;
                                        }
                                        compressed = new int[minList.length + 1024];
                                        inputoffset = new IntWrapper(0);
                                        outputoffset = new IntWrapper(0);
                                        codec.compress(minList, inputoffset, minList.length, compressed, outputoffset);
                                        compressed = Arrays.copyOf(compressed, outputoffset.intValue());
                                        for (i = compressed.length - 1; i >= 0; i--) {
                                            outBlock.add(0, compressed[i]);
                                        }
                                        outBlock.add(0, minList.length);
                                        outBlock.add(0, compressed.length);

                                        int[] outBlock2 = new int[outBlock.size()];
                                        i = 0;
                                        for (int value : outBlock) {
                                            outBlock2[i] = value;
                                            i++;
                                        }
                                        encodeTime += System.nanoTime() - s;
                                        s = System.nanoTime();
                                        offset = 0;
                                        int[] uncompressed = new int[data.size()];
                                        int j = 0;
                                        int k = 0;
                                        int first_slice_size = outBlock.get(offset);
                                        int first_endindex = offset + 2 + first_slice_size;
                                        int min_origin_size = outBlock.get(offset + 1);
                                        int[] new_min = new int[min_origin_size];
                                        int[] min_sliceBlock = Arrays.copyOfRange(outBlock2, offset + 2, first_endindex);
                                        IntWrapper recoffset = new IntWrapper(0);
                                        codec.uncompress(min_sliceBlock, new IntWrapper(0), min_sliceBlock.length,
                                                new_min, recoffset);
                                        offset = first_endindex;
                                        while (offset != outBlock.size()) {
                                            //RLE---
                                            int slice_size = outBlock.get(offset);
                                            int endindex = offset + 2 + slice_size;
                                            int origin_size = outBlock.get(offset + 1);
                                            int[] sliceBlock = Arrays.copyOfRange(outBlock2, offset + 2, endindex);
                                            int[] recovered = new int[origin_size];
                                            recoffset = new IntWrapper(0);
                                            codec.uncompress(sliceBlock, new IntWrapper(0), sliceBlock.length,
                                                    recovered, recoffset);
                                            offset = endindex;
                                            slice_size = outBlock.get(offset);
                                            endindex = offset + 2 + slice_size;
                                            int origin_size2 = outBlock.get(offset + 1);
                                            int[] recovered2 = new int[origin_size2];
                                            recoffset = new IntWrapper(0);
                                            int[] sliceBlock2 = Arrays.copyOfRange(outBlock2, offset + 2, endindex);
                                            codec.uncompress(sliceBlock2, new IntWrapper(0), sliceBlock2.length,
                                                    recovered2, recoffset);

                                            int index2 = 0;
                                            int index1;
                                            int fake_index = 0;
                                            for (index1 = 0; index1 < origin_size; index1++) {
                                                if (index2 < recovered2.length && fake_index == recovered2[index2]) {
                                                    //repeat
                                                    int times = recovered2[index2 + 1];
                                                    for (i = 0; i < times; i++) {
                                                        uncompressed[j] = recovered[index1] + new_min[k];
                                                        j++;
                                                    }
                                                    fake_index += recovered2[index2 + 1];
                                                    index2 += 2;
                                                } else {
                                                    uncompressed[j] = recovered[index1] + new_min[k];
                                                    j++;
                                                    fake_index++;
                                                }
                                            }
                                            k++;
                                            offset = endindex;
                                        }
                                        decodeTime += System.nanoTime() - s;


                                        final_ratio += 1.0 * outBlock.size() / origin.length;
                                        final_compressed_size += outBlock.size() * 4;
                                    }
                                }

                                String[] record = {
                                        f.toString(),
                                        "1",
                                        encoding0 + "+" + encoding,
                                        "NOCOMPRESSION",
                                        String.valueOf(1.0 * encodeTime / fileRepeat),
                                        String.valueOf(1.0 * decodeTime / fileRepeat),
                                        "0",
                                        "0",
                                        String.valueOf(points),
                                        String.valueOf(final_compressed_size / fileRepeat),
                                        String.valueOf(final_ratio / fileRepeat)
                                };
                                writer.writeRecord(record);
                                break;
                            }
                            case "TS_2DIFF1": {
                                fileRepeat = 0;
                                double final_ratio = 0;
                                int final_compressed_size = 0;
                                System.out.println(f);
                                long encodeTime = 0;
                                long decodeTime = 0;

                                while (fileRepeat < 1) {
                                    fileRepeat += 1;
                                    InputStream inputStream = Files.newInputStream(f.toPath());
                                    CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                                    ArrayList<String> data = new ArrayList<>();

                                    for (int index : columnIndexes) {
                                        if (index == 0) {
                                            continue;
                                        }
                                        loader.readHeaders();
                                        data.clear();
                                        while (loader.readRecord()) {
                                            String v = loader.getValues()[index];
                                            data.add(v);
                                        }
                                        points = data.size();
                                        inputStream.close();

                                        int[] origin = new int[data.size()];
                                        int i = 0;
                                        for (String value : data) {
                                            origin[i++] = Integer.parseInt(value);
                                        }
                                        // TS_2DIFF
                                        inputoffset = new IntWrapper(0);
                                        outputoffset = new IntWrapper(0);
                                        compressed = new int[origin.length + 1024];
                                        codec.compress(origin, inputoffset, origin.length, compressed, outputoffset);
                                        long s = System.nanoTime();
                                        compressed = Arrays.copyOf(compressed, outputoffset.intValue());
                                        encodeTime += System.nanoTime() - s;
                                        s = System.nanoTime();
                                        int[] uncompressed = new int[data.size()];
                                        IntWrapper recoffset = new IntWrapper(0);
                                        codec.uncompress(compressed, new IntWrapper(0), compressed.length,
                                                uncompressed, recoffset);
                                        decodeTime += System.nanoTime() - s;


                                        final_ratio += 1.0 * compressed.length / origin.length;
                                        final_compressed_size += compressed.length * 4;
                                    }
                                }

                                String[] record = {
                                        f.toString(),
                                        "1",
                                        encoding0 + "+" + encoding,
                                        "NOCOMPRESSION",
                                        String.valueOf(1.0 * encodeTime / fileRepeat),
                                        String.valueOf(1.0 * decodeTime / fileRepeat),
                                        "0",
                                        "0",
                                        String.valueOf(points),
                                        String.valueOf(final_compressed_size / fileRepeat),
                                        String.valueOf(final_ratio / fileRepeat)
                                };
                                writer.writeRecord(record);
                                break;
                            }
                        }
                    }
                }
            }
            writer.close();
        }
    }
}
