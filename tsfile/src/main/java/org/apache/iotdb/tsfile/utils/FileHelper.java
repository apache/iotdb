package org.apache.iotdb.tsfile.utils;

import it.unimi.dsi.util.XoRoShiRo128PlusRandom;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.Arrays;

public class FileHelper {
  public static BufferedReader READ(String filename) throws IOException {
    File file = new File(filename);
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(new FileInputStream(file)), 50 * 1024 * 1024);
    //        reader.mark((int) (file.length() + 1));
    return reader;
  }

  public static double[] READ(String filename, int read_count, int total_count) throws IOException {

    XoRoShiRo128PlusRandom random = new XoRoShiRo128PlusRandom(2333);
    //        Random random = new Random();
    double threshold = read_count * 1.0 / total_count;
    double[] nums = new double[read_count];
    int number = 0;
    while (number < nums.length) {
      BufferedReader br = READ(filename);
      br.readLine();
      String line;
      if (threshold < 1.0 - 1e-9)
        for (; number < nums.length && (line = br.readLine()) != null; ++number) {
          if (random.nextDoubleFast() < threshold) {
            nums[number] = Double.parseDouble(line);
          } else {
            number--;
          }
        }
      else
        for (; number < nums.length && (line = br.readLine()) != null; ++number) {
          nums[number] = Double.parseDouble(line);
        }
      br.close();
    }
    return nums;
  }

  public static void COMBINE_FILE(String pattern, String distribution) throws IOException {
    String filename = String.format(pattern, distribution, 0);
    BufferedReader br = FileHelper.READ(filename);
    String[] headers = StringUtils.split(br.readLine(), ',');
    int cols = headers.length, rows = 0;
    while (br.readLine() != null) {
      rows++;
    }
    br.close();
    double[][] cache = new double[rows][cols];

    for (int i = 0; i <= 9; ++i) {
      filename = String.format(pattern, distribution, i);
      br = FileHelper.READ(filename);
      br.readLine();

      for (int r = 0; r < rows; ++r) {
        Double[] nums =
            Arrays.stream(br.readLine().split(",", -1))
                .map(
                    s -> {
                      if (s.isEmpty()) {
                        return 0.0;
                      } else {
                        return Double.parseDouble(s);
                      }
                    })
                .toArray(Double[]::new);
        for (int c = 0; c < cols; ++c) {
          cache[r][c] += nums[c];
        }
      }
    }

    filename = String.format(pattern, distribution, 20).replace("20", "");
    FileWriter fw = new FileWriter(filename);
    fw.write(StringUtils.join(headers, ',') + "\n");
    for (int r = 0; r < rows; ++r) {
      for (int c = 0; c < cols; ++c) {
        fw.write((cache[r][c] > 0 ? (cache[r][c] / 10) : "") + ((c == cols - 1) ? "\n" : ","));
      }
    }
    fw.close();
  }

  public static String ADD_SUFFIX(String s, char sep, String suffix) {
    String[] tmp = StringUtils.split(s, sep);
    tmp[0] += suffix;
    return StringUtils.join(tmp, sep);
  }

  public static int GET_SIZE(Object v) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream oos;
    try {
      oos = new ObjectOutputStream(bos);
      oos.writeObject(v);
      oos.flush();
      oos.close();
      bos.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return bos.toByteArray().length;
  }
}
