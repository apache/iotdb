package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.apache.iotdb.tsfile.read.filter.operator.In;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class KernelDensityEstimation {
        public static void main(String[] args) {

        }

        static double[] calculateKernelDensity(Map<Integer, Integer> discreteDistribution) {
            int maxKey = discreteDistribution.keySet().stream().max(Integer::compare).orElse(0);
            double[] kernelDensity = new double[maxKey];

            for (int x = 1; x <= maxKey; x++) {
                for (Map.Entry<Integer, Integer> entry : discreteDistribution.entrySet()) {
                    int dataPoint = entry.getKey();
                    int weight = entry.getValue();
                    kernelDensity[x - 1] += gaussianKernel(x, dataPoint) * weight;
                }
            }

            return kernelDensity;
        }


        private static double gaussianKernel(int x, int dataPoint) {
            double bandwidth = 1.0;
            return Math.exp(-0.5 * Math.pow((x - dataPoint) / bandwidth, 2)) / (Math.sqrt(2 * Math.PI) * bandwidth);
        }

        static int[] findMinIndex(double[] array) {
            int[] minIndex = new int[array.length];
            int final_min_count = 0;
            int pre_value = 0;

            for (int i = 1; i < array.length-1; i++) {
                if (array[i] < array[i-1] && array[i] < array[i+1]) {
                    if(final_min_count != 0){
                        if(i>pre_value+32){
                            minIndex[final_min_count] = i;
                            final_min_count ++;
                            pre_value = i;
                        }
                    }else{
                        minIndex[final_min_count] = i;
                        final_min_count ++;
                        pre_value = i;
                    }
                }
            }
            int[] final_minIndex = new int[final_min_count];

            System.arraycopy(minIndex, 0, final_minIndex, 0, final_min_count);
//            }
            return final_minIndex;
        }

    public static void calculate(int[] data, int block_size) {
        Map<Integer, Integer> data_map = new HashMap<>();
        int[] ts_block;
        int[] third_value;
        ts_block = new int[block_size];
        int i = 0;
        int min_value = Integer.MAX_VALUE;
        for (int j = 0; j < block_size; j++) {
            ts_block[j] = data[j + i * block_size];
            if(ts_block[j]<min_value){
                min_value = ts_block[j];
            }
            if(data_map.containsKey(ts_block[j])){
                int tmp = data_map.get(ts_block[j]);
                tmp++;
                data_map.put(ts_block[j],tmp);
            }else{
                data_map.put(ts_block[j],1);
            }
        }
        double[] kernelDensity = calculateKernelDensity(data_map);
        third_value= findMinIndex(kernelDensity);

        System.out.println("Minimum point: x=" + (Arrays.toString(third_value)));
    }

    @Test
    public void CalParameter() throws IOException {
        String input_parent_dir = "E:\\encoding-reorder-icde\\vldb\\iotdb_datasets_lists\\";
        ArrayList<String> input_path_list = new ArrayList<>();
        ArrayList<String> dataset_name = new ArrayList<>();

        //dataset_name.add("FANYP-Sensors");
        dataset_name.add("TRAJET-Transport");
        for (String value : dataset_name) {
            input_path_list.add(input_parent_dir + value);
        }

        for (int file_i = 0; file_i < input_path_list.size(); file_i++) {
            String inputPath = input_path_list.get(file_i);
            File file = new File(inputPath);
            File[] tempList = file.listFiles();
            assert tempList != null;

            for (File f : tempList) {
                System.out.println(f);
                InputStream inputStream = Files.newInputStream(f.toPath());
                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                ArrayList<ArrayList<Integer>> data = new ArrayList<>();

                loader.readHeaders();
                while (loader.readRecord()) {
                    ArrayList<Integer> tmp = new ArrayList<>();
                    //tmp.add(Integer.valueOf(loader.getValues()[0]));
                    tmp.add(0);
                    tmp.add(Integer.valueOf(loader.getValues()[1]));
                    data.add(tmp);
                }
                inputStream.close();

                int[] data_arr = new int[data.size()];
                for (int i = 0; i < data.size(); i++) {
                    data_arr[i] = data.get(i).get(1);
                }
                calculate(data_arr, 128);
            }
        }
    }
}

