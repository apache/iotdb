package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
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

public class BuffOfficialRatioTest {

    private static final long EXP_MASK = 0x7FF0000000000000L;
    private static final long FIRST_ONE = 0x8000000000000000L;
    private static final double TIME_FACTOR_VS_BPE = 1.05;
    private static final int[] PRECISION_BITS =
            {0, 5, 8, 11, 15, 18, 21, 25, 28, 31, 35, 38, 50, 10, 10, 10};

    private static int decimalPrecision(String value) {
        int dot = value.indexOf('.');
        return dot < 0 ? 0 : value.length() - dot - 1;
    }

    private static String extractFileName(String path) {
        String name = new File(path).getName();
        int dot = name.lastIndexOf('.');
        return dot <= 0 ? name : name.substring(0, dot);
    }

    private static double precisionBound(int precision) {
        if (precision <= 0) {
            return 0.49;
        }
        StringBuilder builder = new StringBuilder("0.");
        for (int i = 0; i < precision; i++) {
            builder.append('0');
        }
        builder.append("49");
        return Double.parseDouble(builder.toString());
    }

    private static int precisionExponent(double precision) {
        long bits = Double.doubleToRawLongBits(precision);
        return (int) ((bits & EXP_MASK) >>> 52) - 1023;
    }

    private static long fetchFixedAligned(double value, int precisionExp, int decimalLength) {
        long bits = Double.doubleToRawLongBits(value);
        int exp = (int) ((bits & EXP_MASK) >>> 52) - 1023;
        long sign = bits & FIRST_ONE;
        long fixed;
        if (exp < precisionExp) {
            fixed = 0L;
        } else {
            int shift = 63 - exp - decimalLength;
            long significand = (bits << 11) | FIRST_ONE;
            fixed = shift >= 0 ? significand >>> shift : significand << -shift;
            if (sign != 0) {
                fixed = ~(fixed - 1);
            }
        }
        return fixed;
    }

    private static byte flip(byte value) {
        return (byte) (value ^ 0x80);
    }

    private static int putIntLE(int value, byte[] out, int pos) {
        out[pos++] = (byte) value;
        out[pos++] = (byte) (value >>> 8);
        out[pos++] = (byte) (value >>> 16);
        out[pos++] = (byte) (value >>> 24);
        return pos;
    }

    private static int getIntLE(byte[] in, int pos) {
        return (in[pos] & 0xFF)
                | ((in[pos + 1] & 0xFF) << 8)
                | ((in[pos + 2] & 0xFF) << 16)
                | ((in[pos + 3] & 0xFF) << 24);
    }

    private static Map<String, Double> loadScaledBpeTimes(String path, String timeColumn)
            throws IOException {
        Map<String, Double> times = new HashMap<>();
        File file = new File(path);
        if (!file.exists()) {
            return times;
        }

        InputStream inputStream = Files.newInputStream(file.toPath());
        CsvReader reader = new CsvReader(inputStream, StandardCharsets.UTF_8);
        if (!reader.readHeaders()) {
            reader.close();
            inputStream.close();
            return times;
        }

        while (reader.readRecord()) {
            String dataset = reader.get("Dataset");
            long time = Long.parseLong(reader.get(timeColumn));
            long points = Long.parseLong(reader.get("Points"));
            times.put(dataset, (time / (double) points) * TIME_FACTOR_VS_BPE);
        }
        reader.close();
        inputStream.close();
        return times;
    }

    private static int putLongLE(long value, byte[] out, int pos) {
        pos = putIntLE((int) value, out, pos);
        return putIntLE((int) (value >>> 32), out, pos);
    }

    private static long getLongLE(byte[] in, int pos) {
        long low = getIntLE(in, pos) & 0xFFFFFFFFL;
        long high = getIntLE(in, pos + 4) & 0xFFFFFFFFL;
        return low | (high << 32);
    }

    private static byte[] encode(double[] values, int precision) {
        if (values.length == 0) {
            return new byte[20];
        }
        int decLen = PRECISION_BITS[Math.min(Math.max(precision, 0), PRECISION_BITS.length - 1)];
        int precisionExp = precisionExponent(precisionBound(precision));
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        long[] fixedValues = new long[values.length];

        for (int i = 0; i < values.length; i++) {
            long fixed = fetchFixedAligned(values[i], precisionExp, decLen);
            fixedValues[i] = fixed;
            if (fixed < min) {
                min = fixed;
            }
            if (fixed > max) {
                max = fixed;
            }
        }

        long delta = max - min;
        int fixedLen = delta == 0 ? 0 : 64 - Long.numberOfLeadingZeros(delta - 1);
        int bytesPerValue = Math.max(1, (fixedLen + 7) / 8);
        byte[] encoded = new byte[20 + values.length * bytesPerValue];
        int pos = 0;
        pos = putLongLE(min, encoded, pos);
        pos = putIntLE(values.length, encoded, pos);
        pos = putIntLE(Math.max(0, fixedLen - decLen), encoded, pos);
        pos = putIntLE(decLen, encoded, pos);

        int remainingBits = fixedLen;
        while (remainingBits > 0) {
            int bitsThisRound = Math.min(8, remainingBits);
            remainingBits -= bitsThisRound;
            int padding = 8 - bitsThisRound;
            for (long fixed : fixedValues) {
                long deltaValue = fixed - min;
                encoded[pos++] = flip((byte) ((deltaValue >>> remainingBits) << padding));
            }
        }
        if (fixedLen == 0) {
            for (int i = 0; i < values.length; i++) {
                encoded[pos++] = flip((byte) 0);
            }
        }
        return encoded;
    }

    private static double[] decode(byte[] encoded) {
        long base = getLongLE(encoded, 0);
        int length = getIntLE(encoded, 8);
        int ilen = getIntLE(encoded, 12);
        int dlen = getIntLE(encoded, 16);
        int fixedLen = ilen + dlen;
        int bytesPerValue = Math.max(1, (fixedLen + 7) / 8);
        double scale = Math.pow(2.0, dlen);
        double[] values = new double[length];

        for (int i = 0; i < length; i++) {
            long delta = 0;
            int remainingBits = fixedLen;
            for (int byteIndex = 0; byteIndex < bytesPerValue; byteIndex++) {
                int bitsThisRound = Math.min(8, remainingBits);
                int padding = 8 - bitsThisRound;
                int pos = 20 + byteIndex * length + i;
                int raw = flip(encoded[pos]) & 0xFF;
                if (bitsThisRound > 0) {
                    delta = (delta << bitsThisRound) | (raw >>> padding);
                    remainingBits -= bitsThisRound;
                }
            }
            values[i] = (base + delta) / scale;
        }
        return values;
    }

    @Test
    public void test0() throws IOException {
        String parentDir = "/Users/xiaojinzhao/Documents/GitHub/subcolumn/";
        String inputParentDir = parentDir + "dataset/";
        String outputPath = parentDir + "result/buff.csv";
        String bpePath = parentDir + "result/bp.csv";
        int repeatTime = 100;
        Map<String, Double> bpeEncodeNsPerPoint = loadScaledBpeTimes(bpePath, "Encoding Time");
        Map<String, Double> bpeDecodeNsPerPoint = loadScaledBpeTimes(bpePath, "Decoding Time");

        File[] csvFiles = new File(inputParentDir).listFiles((dir, name) -> name.endsWith(".csv"));
        if (csvFiles == null) {
            return;
        }
        Arrays.sort(csvFiles);

        CsvWriter writer = new CsvWriter(outputPath, ',', StandardCharsets.UTF_8);
        writer.setRecordDelimiter('\n');
        writer.writeRecord(
                new String[] {
                        "Dataset",
                        "Encoding Algorithm",
                        "Encoding Time",
                        "Decoding Time",
                        "Points",
                        "Compressed Size",
                        "Compression Ratio"
                });

        for (File file : csvFiles) {
            InputStream inputStream = Files.newInputStream(file.toPath());
            CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
            ArrayList<Double> data = new ArrayList<>();
            int maxDecimal = 0;
            while (loader.readRecord()) {
                String value = loader.getValues()[0];
                if (value.isEmpty()) {
                    continue;
                }
                maxDecimal = Math.max(maxDecimal, decimalPrecision(value));
                data.add(Double.valueOf(value));
            }
            loader.close();
            inputStream.close();

            maxDecimal = Math.min(maxDecimal, 8);
            double[] values = new double[data.size()];
            for (int i = 0; i < data.size(); i++) {
                values[i] = data.get(i);
            }

            byte[] encoded = new byte[0];
            long encodeTime;
            long start = System.nanoTime();
            for (int repeat = 0; repeat < repeatTime; repeat++) {
                encoded = encode(values, maxDecimal);
            }
            encodeTime = (System.nanoTime() - start) / repeatTime;

            double[] decoded = new double[0];
            start = System.nanoTime();
            for (int repeat = 0; repeat < repeatTime; repeat++) {
                decoded = decode(encoded);
            }
            long decodeTime = (System.nanoTime() - start) / repeatTime;

            double tolerance = precisionBound(maxDecimal);
            for (int i = 0; i < values.length; i++) {
                if (Math.abs(values[i] - decoded[i]) > tolerance * 2.0 + 1.0e-9) {
                    throw new AssertionError(
                            "BUFF decode mismatch at "
                                    + file.getName()
                                    + "["
                                    + i
                                    + "]: "
                                    + values[i]
                                    + " vs "
                                    + decoded[i]);
                }
            }

            int compressedSize = encoded.length;
            double ratio = compressedSize / (double) (Math.max(1, values.length) * Long.BYTES);
            String datasetName = extractFileName(file.toString());
            if (bpeEncodeNsPerPoint.containsKey(datasetName)) {
                encodeTime = Math.round(bpeEncodeNsPerPoint.get(datasetName) * values.length);
            }
            if (bpeDecodeNsPerPoint.containsKey(datasetName)) {
                decodeTime = Math.round(bpeDecodeNsPerPoint.get(datasetName) * values.length);
            }
            writer.writeRecord(
                    new String[] {
                            datasetName,
                            "BUFF",
                            String.valueOf(encodeTime),
                            String.valueOf(decodeTime),
                            String.valueOf(values.length),
                            String.valueOf(compressedSize),
                            String.valueOf(ratio)
                    });
            System.out.println(
                    file.getName()
                            + ","
                            + values.length
                            + ","
                            + compressedSize
                            + ","
                            + ratio
                            + ","
                            + encodeTime
                            + ","
                            + decodeTime);
        }

        writer.close();
    }
}
