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
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class SubcolumnLongPSOTest {

    public static int bitWidth(int value) {
        return 32 - Integer.numberOfLeadingZeros(value);
    }

    public static int bitWidth(long value) {
        return 64 - Long.numberOfLeadingZeros(value);
    }

    public static void intToBytes(int srcNum, byte[] result, int pos, int width) {
        int cnt = pos & 0x07;
        int index = pos >> 3;
        while (width > 0) {
            int m = width + cnt >= 8 ? 8 - cnt : width;
            width -= m;
            int mask = 1 << (8 - cnt);
            cnt += m;
            byte y = (byte) (srcNum >>> width);
            y = (byte) (y << (8 - cnt));
            mask = ~(mask - (1 << (8 - cnt)));
            result[index] = (byte) (result[index] & mask | y);
            srcNum = srcNum & ~(-1 << width);
            if (cnt == 8) {
                index++;
                cnt = 0;
            }
        }
    }

    public static int bytesToInt(byte[] result, int pos, int width) {
        int ret = 0;
        int cnt = pos & 0x07;
        int index = pos >> 3;
        while (width > 0) {
            int m = width + cnt >= 8 ? 8 - cnt : width;
            width -= m;
            ret = ret << m;
            byte y = (byte) (result[index] & (0xff >> cnt));
            y = (byte) ((y & 0xff) >>> (8 - cnt - m));
            ret = ret | (y & 0xff);
            cnt += m;
            if (cnt == 8) {
                cnt = 0;
                index++;
            }
        }
        return ret;
    }

    public static void longToBytes(long srcNum, byte[] result, int pos, int width) {
        int cnt = pos & 0x07;
        int index = pos >> 3;

        while (width > 0) {
            int m = width + cnt >= 8 ? 8 - cnt : width;
            width -= m;
            int mask = 1 << (8 - cnt);
            cnt += m;
            byte y = (byte) (srcNum >>> width);
            y = (byte) (y << (8 - cnt));
            mask = ~(mask - (1 << (8 - cnt)));
            result[index] = (byte) (result[index] & mask | y);
            srcNum = srcNum & ~(-1L << width);
            if (cnt == 8) {
                index++;
                cnt = 0;
            }
        }
    }

    public static long bytesToLong(byte[] result, int pos, int width) {
        long ret = 0;
        int cnt = pos & 0x07;
        int index = pos >> 3;
        while (width > 0) {
            int m = width + cnt >= 8 ? 8 - cnt : width;
            width -= m;
            ret = ret << m;
            byte y = (byte) (result[index] & (0xff >> cnt));
            y = (byte) ((y & 0xff) >>> (8 - cnt - m));
            ret = ret | (y & 0xff);
            cnt += m;
            if (cnt == 8) {
                cnt = 0;
                index++;
            }
        }
        return ret;
    }

    public static void pack8Values(int[] values, int offset, int width, int encode_pos,
            byte[] encoded_result) {
        int bufIdx = 0;
        int valueIdx = offset;
        // remaining bits for the current unfinished Integer
        int leftBit = 0;

        while (valueIdx < 8 + offset) {
            // buffer is used for saving 32 bits as a part of result
            int buffer = 0;
            // remaining size of bits in the 'buffer'
            int leftSize = 32;

            // encode the left bits of current Integer to 'buffer'
            if (leftBit > 0) {
                buffer |= (values[valueIdx] << (32 - leftBit));
                leftSize -= leftBit;
                leftBit = 0;
                valueIdx++;
            }

            while (leftSize >= width && valueIdx < 8 + offset) {
                // encode one Integer to the 'buffer'
                buffer |= (values[valueIdx] << (leftSize - width));
                leftSize -= width;
                valueIdx++;
            }
            // If the remaining space of the buffer can not save the bits for one Integer,
            if (leftSize > 0 && valueIdx < 8 + offset) {
                // put the first 'leftSize' bits of the Integer into remaining space of the
                // buffer
                buffer |= (values[valueIdx] >>> (width - leftSize));
                leftBit = width - leftSize;
            }

            // put the buffer into the final result
            for (int j = 0; j < 4; j++) {
                encoded_result[encode_pos] = (byte) ((buffer >>> ((3 - j) * 8)) & 0xFF);
                encode_pos++;
                bufIdx++;
                if (bufIdx >= width) {
                    return;
                }
            }
        }

    }

    public static void pack8Values(
            long[] values, int offset, int width, int encode_pos, byte[] encoded_result) {
        int bufIdx = 0;
        int valueIdx = offset;
        // remaining bits for the current unfinished Long
        int leftBit = 0;

        while (valueIdx < 8 + offset) {
            // buffer is used for saving 64 bits as a part of result
            long buffer = 0;
            // remaining size of bits in the 'buffer'
            int leftSize = 64;

            // encode the left bits of current Long to 'buffer'
            if (leftBit > 0) {
                buffer |= (values[valueIdx] << (64 - leftBit));
                leftSize -= leftBit;
                leftBit = 0;
                valueIdx++;
            }

            while (leftSize >= width && valueIdx < 8 + offset) {
                // encode one Long to the 'buffer'
                buffer |= (values[valueIdx] << (leftSize - width));
                leftSize -= width;
                valueIdx++;
            }
            // If the remaining space of the buffer can not save the bits for one Long
            if (leftSize > 0 && valueIdx < 8 + offset) {
                // put the first 'leftSize' bits of the Long into remaining space of the buffer
                buffer |= (values[valueIdx] >>> (width - leftSize));
                leftBit = width - leftSize;
            }

            // put the buffer into the final result
            for (int j = 0; j < 8; j++) {
                encoded_result[encode_pos] = (byte) ((buffer >>> ((7 - j) * 8)) & 0xFF);
                encode_pos++;
                bufIdx++;
                if (bufIdx >= width) {
                    return;
                }
            }
        }
    }

    public static void unpack8Values(byte[] encoded, int offset, int width, int[] result_list, int result_offset) {
        int byteIdx = offset;
        long buffer = 0;
        // total bits which have read from 'buf' to 'buffer'. i.e.,
        // number of available bits to be decoded.
        int totalBits = 0;
        int valueIdx = 0;

        while (valueIdx < 8) {
            // If current available bits are not enough to decode one Integer,
            // then add next byte from buf to 'buffer' until totalBits >= width
            while (totalBits < width) {
                buffer = (buffer << 8) | (encoded[byteIdx] & 0xFF);
                byteIdx++;
                totalBits += 8;
            }

            // If current available bits are enough to decode one Integer,
            // then decode one Integer one by one until left bits in 'buffer' is
            // not enough to decode one Integer.
            while (totalBits >= width && valueIdx < 8) {
                // result_list.add((int) (buffer >>> (totalBits - width)));
                result_list[result_offset + valueIdx] = (int) (buffer >>> (totalBits - width));
                valueIdx++;
                totalBits -= width;
                buffer = buffer & ((1L << totalBits) - 1);
            }
        }
    }

    public static void unpack8Values(
            byte[] encoded, int offset, int width, long[] result_list, int result_offset) {
        int byteIdx = offset;
        long buffer = 0;
        int totalBits = 0;
        int valueIdx = 0;

        while (valueIdx < 8) {
            // If current available bits are not enough to decode one Integer,
            // then add next byte from buf to 'buffer' until totalBits >= width
            while (totalBits < width) {
                buffer = (buffer << 8) | (encoded[byteIdx] & 0xFF);
                byteIdx++;
                totalBits += 8;
            }

            // If current available bits are enough to decode one Integer,
            // then decode one Integer one by one until left bits in 'buffer' is
            // not enough to decode one Integer.
            while (totalBits >= width && valueIdx < 8) {
                // result_list.add((int) (buffer >>> (totalBits - width)));
                result_list[result_offset + valueIdx] = buffer >>> (totalBits - width);
                valueIdx++;
                totalBits -= width;
                buffer = buffer & ((1L << totalBits) - 1);
            }
        }
    }

    public static int bitPacking(int[] numbers, int bit_width, int encode_pos,
            byte[] encoded_result, int num_values) {
        int block_num = num_values / 8;
        int remainder = num_values % 8;

        for (int i = 0; i < block_num; i++) {
            pack8Values(numbers, i * 8, bit_width, encode_pos, encoded_result);
            encode_pos += bit_width;
        }

        encode_pos *= 8;

        for (int i = 0; i < remainder; i++) {
            intToBytes(numbers[block_num * 8 + i], encoded_result, encode_pos, bit_width);
            encode_pos += bit_width;
        }

        return (encode_pos + 7) / 8;
    }

    public static int bitPacking(long[] numbers, int bit_width, int encode_pos,
            byte[] encoded_result, int num_values) {
        int block_num = num_values / 8;
        int remainder = num_values % 8;

        for (int i = 0; i < block_num; i++) {
            pack8Values(numbers, i * 8, bit_width, encode_pos, encoded_result);
            encode_pos += bit_width;
        }

        encode_pos *= 8;

        for (int i = 0; i < remainder; i++) {
            longToBytes(numbers[block_num * 8 + i], encoded_result, encode_pos, bit_width);
            encode_pos += bit_width;
        }

        return (encode_pos + 7) / 8;
    }

    public static int decodeBitPacking(
            byte[] encoded, int decode_pos, int bit_width, int num_values, int[] result_list) {
        int block_num = num_values / 8;
        int remainder = num_values % 8;

        for (int i = 0; i < block_num; i++) {
            unpack8Values(encoded, decode_pos, bit_width, result_list, i * 8);
            decode_pos += bit_width;
        }

        decode_pos *= 8;

        for (int i = 0; i < remainder; i++) {
            result_list[block_num * 8 + i] = bytesToInt(encoded, decode_pos, bit_width);
            decode_pos += bit_width;
        }

        return (decode_pos + 7) / 8;
    }

    public static int decodeBitPacking(
            byte[] encoded, int decode_pos, int bit_width, int num_values, long[] result_list) {
        int block_num = num_values / 8;
        int remainder = num_values % 8;

        for (int i = 0; i < block_num; i++) {
            unpack8Values(encoded, decode_pos, bit_width, result_list, i * 8);
            decode_pos += bit_width;
        }

        decode_pos *= 8;

        for (int i = 0; i < remainder; i++) {
            result_list[block_num * 8 + i] = bytesToLong(encoded, decode_pos, bit_width);
            decode_pos += bit_width;
        }

        return (decode_pos + 7) / 8;
    }

    public static void int2Bytes(int integer, int encode_pos, byte[] cur_byte) {
        cur_byte[encode_pos] = (byte) (integer >> 24);
        cur_byte[encode_pos + 1] = (byte) (integer >> 16);
        cur_byte[encode_pos + 2] = (byte) (integer >> 8);
        cur_byte[encode_pos + 3] = (byte) (integer);
    }

    public static void intByte2Bytes(int integer, int encode_pos, byte[] cur_byte) {
        cur_byte[encode_pos] = (byte) (integer);
    }

    public static void long2Bytes(long integer, int encode_pos, byte[] cur_byte) {
        cur_byte[encode_pos] = (byte) (integer >> 56);
        cur_byte[encode_pos + 1] = (byte) (integer >> 48);
        cur_byte[encode_pos + 2] = (byte) (integer >> 40);
        cur_byte[encode_pos + 3] = (byte) (integer >> 32);
        cur_byte[encode_pos + 4] = (byte) (integer >> 24);
        cur_byte[encode_pos + 5] = (byte) (integer >> 16);
        cur_byte[encode_pos + 6] = (byte) (integer >> 8);
        cur_byte[encode_pos + 7] = (byte) (integer);
    }

    public static int bytes2Integer(byte[] encoded, int start, int num) {
        int value = 0;

        for (int i = 0; i < num; i++) {
            value <<= 8;
            int b = encoded[i + start] & 0xFF;
            value |= b;
        }
        return value;
    }

    public static long bytes2Long(byte[] encoded, int start, int num) {
        long value = 0;

        for (int i = 0; i < num; i++) {
            value <<= 8;
            int b = encoded[i + start] & 0xFF;
            value |= b;
        }
        return value;
    }

// ------------------ PSO-based Subcolumn (replace original Subcolumn) ------------------

    /**
     * Compute the total storage cost for a given beta following original cost logic.
     * This mirrors the cost calculation in the original Subcolumn implementation.
     */
    private static int computeCostForBeta(long[] x, int x_length, int m, int block_size, int beta) {
        // clamp beta to [1, m]
        if (beta < 1) beta = 1;
        if (beta > m) beta = m;

        int l = (m + beta - 1) / beta;
        long[][] subcolumnList = new long[l][x_length];
        int[] bitWidthList = new int[l];

        int maskBeta;
        long mask;
        // build subcolumns and bitWidthList
        for (int i = 0; i < l; i++) {
            long maxValuePart = 0;
            int shiftAmount = i * beta;
            // compute mask safely (avoid shifting by >=64)
            if (beta >= 63) {
                mask = ~0L;
            } else {
                mask = ((1L << beta) - 1L);
            }
            for (int j = 0; j < x_length; j++) {
                subcolumnList[i][j] = (x[j] >> shiftAmount) & mask;
                if (subcolumnList[i][j] > maxValuePart) {
                    maxValuePart = subcolumnList[i][j];
                }
            }
            bitWidthList[i] = bitWidth(maxValuePart);
        }

        int bw = bitWidth(block_size);
        int totalCost = 0;

        // for each sub-column compute min(bpCost, rleCost) following original logic
        for (int i = 0; i < l; i++) {
            int bpCost = bitWidthList[i] * x_length;
            int rleCost = 0;

            long currentNumber = subcolumnList[i][0];
            int index = 0;
            boolean bpBest = false;

            for (int j = 1; j < x_length; j++) {
                if (subcolumnList[i][j] != currentNumber) {
                    index++;
                    currentNumber = subcolumnList[i][j];
                }
                // if intermediate RLE cost already >= bpCost, stop (same break condition)
                if (bw * index + bitWidthList[i] * index >= bpCost) {
                    bpBest = true;
                    break;
                }
            }

            if (bpBest) {
                totalCost += bpCost;
                continue;
            }

            // finish computing run count (index currently = number of transitions)
            index++;
            rleCost = bw * index + bitWidthList[i] * index;

            if (bpCost <= rleCost) {
                totalCost += bpCost;
            } else {
                totalCost += rleCost;
            }
        }

        return totalCost;
    }

    /**
     * PSO main routine: search integer beta in [1, m] minimizing computeCostForBeta.
     * Returns best integer beta found.
     * <p>
     * Parameters (tunable):
     *  - swarmSize: number of particles
     *  - maxIter: number of PSO iterations
     *  - w, c1, c2: PSO coefficients
     *  - localSearchRadius: after PSO, perform small local search within +/- radius
     */
    private static int psoFindBestBeta(long[] x, int x_length, int m, int block_size,
                                       int swarmSize, int maxIter, double w, double c1, double c2, int localSearchRadius, long seed) {

        if (m <= 1) {
            return 1;
        }

        Random rand = (seed == 0) ? new Random() : new Random(seed);

        // search range 1..m
        double minPos = 1.0;
        double maxPos = (double) m;

        // particle arrays
        double[] pos = new double[swarmSize];
        double[] vel = new double[swarmSize];
        double[] pbestPos = new double[swarmSize];
        int[] pbestCost = new int[swarmSize];

        // initialize particles
        for (int i = 0; i < swarmSize; i++) {
            pos[i] = minPos + rand.nextDouble() * (maxPos - minPos);
            // initial velocity small random
            vel[i] = (rand.nextDouble() - 0.5) * (maxPos - minPos) * 0.2;
            int intval = (int) Math.round(pos[i]);
            if (intval < 1) intval = 1;
            if (intval > m) intval = m;
            pbestPos[i] = pos[i];
            pbestCost[i] = computeCostForBeta(x, x_length, m, block_size, intval);
        }

        // global best
        int gbestIndex = 0;
        int gbestCost = pbestCost[0];
        double gbestPos = pbestPos[0];
        for (int i = 1; i < swarmSize; i++) {
            if (pbestCost[i] < gbestCost) {
                gbestCost = pbestCost[i];
                gbestPos = pbestPos[i];
                gbestIndex = i;
            }
        }

        double vmax = maxPos; // velocity clamp

        // PSO iterations
        for (int iter = 0; iter < maxIter; iter++) {
            for (int i = 0; i < swarmSize; i++) {
                double r1 = rand.nextDouble();
                double r2 = rand.nextDouble();

                // velocity update
                vel[i] = w * vel[i]
                        + c1 * r1 * (pbestPos[i] - pos[i])
                        + c2 * r2 * (gbestPos - pos[i]);

                // clamp velocity
                if (vel[i] > vmax) vel[i] = vmax;
                if (vel[i] < -vmax) vel[i] = -vmax;

                // position update
                pos[i] += vel[i];

                // clamp position
                if (pos[i] < minPos) {
                    pos[i] = minPos;
                    vel[i] = 0.0;
                }
                if (pos[i] > maxPos) {
                    pos[i] = maxPos;
                    vel[i] = 0.0;
                }

                // evaluate integer beta = round(pos)
                int intval = (int) Math.round(pos[i]);
                if (intval < 1) intval = 1;
                if (intval > m) intval = m;

                int cost = computeCostForBeta(x, x_length, m, block_size, intval);

                // update pbest
                if (cost < pbestCost[i]) {
                    pbestCost[i] = cost;
                    pbestPos[i] = pos[i];
                    // update gbest
                    if (cost < gbestCost) {
                        gbestCost = cost;
                        gbestPos = pos[i];
                        gbestIndex = i;
                    }
                }
            }
            // optionally: you could add inertia damping or early stopping here if desired
        }

        // final integer best
        int bestBeta = (int) Math.round(gbestPos);
        if (bestBeta < 1) bestBeta = 1;
        if (bestBeta > m) bestBeta = m;

        // small local search around bestBeta to refine (try +/- localSearchRadius)
        int bestCost = computeCostForBeta(x, x_length, m, block_size, bestBeta);
        int start = Math.max(1, bestBeta - localSearchRadius);
        int end = Math.min(m, bestBeta + localSearchRadius);
        for (int b = start; b <= end; b++) {
            int c = computeCostForBeta(x, x_length, m, block_size, b);
            if (c < bestCost) {
                bestCost = c;
                bestBeta = b;
            }
        }

        return bestBeta;
    }

    /**
     * PSO-based Subcolumn entry point (replaces original Subcolumn).
     * Uses default PSO hyperparameters similar to typical settings.
     */
    public static int Subcolumn(long[] x, int x_length, int m, int block_size) {
        // PSO hyperparameters (you can tune these if needed)
        final int SWARM_SIZE = 3;
        final int MAX_ITER = 2;
        final double W = 0.72;      // inertia
        final double C1 = 1.5;      // cognitive
        final double C2 = 1.5;      // social
        final int LOCAL_RADIUS = 3; // local search radius
        final long SEED = 0L;       // 0 -> use random seed; non-zero -> reproducible


        // trivial cases
        if (x_length == 0) return 1;
        if (m <= 1) return 1;
        m=4;
        // run PSO to find best beta in [1, m]
        int betaBest = psoFindBestBeta(x, x_length, m, block_size,
                SWARM_SIZE, MAX_ITER, W, C1, C2, LOCAL_RADIUS, SEED);

        return betaBest;
    }

    public static int SubcolumnEncoder(long[] list, int encode_pos, byte[] encoded_result, int[] beta, int block_size) {
        int list_length = list.length;
        long maxValue = 0;
        for (int i = 0; i < list_length; i++) {
            if (list[i] > maxValue) {
                maxValue = list[i];
            }
        }

        int m = bitWidth(maxValue);

        intByte2Bytes(m, encode_pos, encoded_result);
        encode_pos += 1;

        if (m == 0) {
            return encode_pos;
        }

        // int[] bitWidthList = new int[m];

        // int[][] subcolumnList = new int[m][list_length];

        int l;

        // int betaBest = beta[0];
        // byte betaBest = (byte) beta[0];

        l = (m + beta[0] - 1) / beta[0];

        int[] bitWidthList = new int[l];

        long[][] subcolumnList = new long[l][list_length];

        intByte2Bytes(beta[0], encode_pos, encoded_result);
        encode_pos += 1;

        int bw = bitWidth(block_size);
        int mask = (1 << beta[0]) - 1;

        for (int i = 0; i < l; i++) {
            long maxValuePart = 0;
            int shiftAmount = i * beta[0];
            for (int j = 0; j < list_length; j++) {
                subcolumnList[i][j] = (list[j] >> shiftAmount) & mask;
                if (subcolumnList[i][j] > maxValuePart) {
                    maxValuePart = subcolumnList[i][j];
                }
            }
            bitWidthList[i] = bitWidth(maxValuePart);
        }

        encode_pos = bitPacking(bitWidthList, 8, encode_pos, encoded_result, l);

        int[] encodingType = new int[l];

        // encoded_result 预留大小为 (l + 7) / 8 的大小，存储每个分列的类型
        int preTypePos = encode_pos;
        encode_pos += (l + 7) / 8;

        for (int i = l - 1; i >= 0; i--) {
            // 对于每个分列，计算使用 bit packing 还是 rle
            int bpCost = bitWidthList[i] * list_length;
            int rleCost = 0;

            long previous = subcolumnList[i][0];
            int index = 0;

            for (int j = 1; j < list_length; j++) {
                long currentNumber = subcolumnList[i][j];
                if (currentNumber != previous) {
                    index++;
                    previous = currentNumber;
                }

                if (bw * index + bitWidthList[i] * index >= bpCost) {
                    break;
                }
            }

            index++;

            rleCost = bw * index + bitWidthList[i] * index;

            if (bpCost <= rleCost) {
                encodingType[i] = 0;

                encode_pos = bitPacking(subcolumnList[i], bitWidthList[i], encode_pos, encoded_result, list_length);

            } else {
                encodingType[i] = 1;

                encoded_result[encode_pos] = (byte) (index >> 8);
                encode_pos += 1;
                encoded_result[encode_pos] = (byte) (index & 0xFF);
                encode_pos += 1;

                index = 0;
                int[] run_length = new int[list_length];
                long[] rle_values = new long[list_length];
                previous = subcolumnList[i][0];

                for (int j = 1; j < list_length; j++) {
                    long currentNumber = subcolumnList[i][j];
                    if (currentNumber != previous) {
                        run_length[index] = j;
                        rle_values[index] = previous;
                        index++;
                        previous = currentNumber;
                    }
                }

                run_length[index] = list_length;
                rle_values[index] = previous;
                index++;

                encode_pos = bitPacking(run_length, bw, encode_pos, encoded_result, index);

                encode_pos = bitPacking(rle_values, bitWidthList[i], encode_pos, encoded_result, index);

            }

        }

        preTypePos = bitPacking(encodingType, 1, preTypePos, encoded_result, l);

        return encode_pos;
    }

    public static int SubcolumnDecoder(byte[] encoded_result, int encode_pos, long[] list, int block_size) {
        int list_length = list.length;

        // int m = encoded_result[encode_pos];
        int m = bytes2Integer(encoded_result, encode_pos, 1);
        encode_pos += 1;

        if (m == 0) {
            return encode_pos;
        }

        int bw = bitWidth(block_size);

        int beta = bytes2Integer(encoded_result, encode_pos, 1);
        encode_pos += 1;

        int l = (m + beta - 1) / beta;

        int[] bitWidthList = new int[l];

        encode_pos = decodeBitPacking(encoded_result, encode_pos, 8, l, bitWidthList);

        long[][] subcolumnList = new long[l][list_length];

        int[] encodingType = new int[l];

        encode_pos = decodeBitPacking(encoded_result, encode_pos, 1, l, encodingType);

        for (int i = l - 1; i >= 0; i--) {
            int type = encodingType[i];
            int bitWidth = bitWidthList[i];
            if (type == 0) {
                encode_pos = decodeBitPacking(encoded_result, encode_pos, bitWidth, list_length,
                        subcolumnList[i]);
            } else {
                int index = ((encoded_result[encode_pos] & 0xFF) << 8) | (encoded_result[encode_pos + 1] & 0xFF);

                encode_pos += 2;

                int[] run_length = new int[index];
                int[] rle_values = new int[index];

                encode_pos = decodeBitPacking(encoded_result, encode_pos, bw, index, run_length);
                encode_pos = decodeBitPacking(encoded_result, encode_pos, bitWidth, index, rle_values);

                int currentIndex = 0;
                for (int j = 0; j < index; j++) {
                    int endPos = run_length[j];
                    int value = rle_values[j];
                    while (currentIndex < endPos) {
                        subcolumnList[i][currentIndex] = value;
                        currentIndex++;
                    }
                }
            }
        }

        for (int i = 0; i < l; i++) {
            int shiftAmount = i * beta;
            for (int j = 0; j < list_length; j++) {
                list[j] |= subcolumnList[i][j] << shiftAmount;
            }
        }

        return encode_pos;
    }

    public static long[] getAbsDeltaTsBlock(
            long[] ts_block,
            int i,
            int block_size,
            int remaining,
            long[] min_delta) {
        long[] ts_block_delta = new long[remaining];

        long value_delta_min = Long.MAX_VALUE;
        long value_delta_max = Long.MIN_VALUE;
        int base = i * block_size;
        int end = i * block_size + remaining;

        for (int j = base; j < end; j++) {
            long cur = ts_block[j];
            if (cur < value_delta_min) {
                value_delta_min = cur;
            }
            if (cur > value_delta_max) {
                value_delta_max = cur;
            }
        }

        for (int j = base; j < end; j++) {
            ts_block_delta[j - base] = ts_block[j] - value_delta_min;
        }

        min_delta[0] = value_delta_min;

        return ts_block_delta;
    }

    public static int BlockEncoder(long[] data, int block_index, int block_size, int remainder,
            int encode_pos, byte[] encoded_result, int[] beta) {
        long[] min_delta = new long[3];

        long[] data_delta = getAbsDeltaTsBlock(data, block_index, block_size,
                remainder, min_delta);

        long2Bytes(min_delta[0], encode_pos, encoded_result);
        encode_pos += 8;

        if (block_index == 0) {
            long maxValue = 0;
            for (int j = 0; j < remainder; j++) {
                if (data_delta[j] > maxValue) {
                    maxValue = data_delta[j];
                }
            }
            int m = bitWidth(maxValue);

            beta[0] = Subcolumn(data_delta, remainder, m, block_size);

            // System.out.println("beta: " + beta[0]);
        }

        encode_pos = SubcolumnEncoder(data_delta, encode_pos,
                encoded_result, beta, block_size);

        return encode_pos;
    }

    public static int BlockDecoder(byte[] encoded_result, int block_index, int block_size, int remainder,
            int encode_pos, long[] data) {
        long[] min_delta = new long[3];

        min_delta[0] = bytes2Long(encoded_result, encode_pos, 8);
        encode_pos += 8;

        long[] block_data = new long[remainder];

        encode_pos = SubcolumnDecoder(encoded_result, encode_pos,
                block_data, block_size);

        for (int i = 0; i < remainder; i++) {
            data[block_index * block_size + i] = block_data[i] + min_delta[0];
        }

        return encode_pos;
    }

    public static int Encoder(long[] data, int block_size, byte[] encoded_result) {
        int data_length = data.length;
        int encode_pos = 0;

        int2Bytes(data_length, encode_pos, encoded_result);
        encode_pos += 4;

        int2Bytes(block_size, encode_pos, encoded_result);
        encode_pos += 4;

        int num_blocks = data_length / block_size;

        int remainder = data_length % block_size;

        int[] beta = new int[1];
        beta[0] = 2;

        for (int i = 0; i < num_blocks; i++) {
            encode_pos = BlockEncoder(data, i, block_size, block_size, encode_pos, encoded_result, beta);
        }

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                long value = data[num_blocks * block_size + i];
                long2Bytes(value, encode_pos, encoded_result);
                encode_pos += 8;
            }
        } else {
            encode_pos = BlockEncoder(data, num_blocks, block_size, remainder, encode_pos,
                    encoded_result, beta);
        }

        // System.out.println("beta: " + beta[0]);

        return encode_pos;
    }

    public static long[] Decoder(byte[] encoded_result) {
        int encode_pos = 0;

        int data_length = bytes2Integer(encoded_result, encode_pos, 4);
        encode_pos += 4;

        int block_size = bytes2Integer(encoded_result, encode_pos, 4);
        encode_pos += 4;

        int num_blocks = data_length / block_size;

        long[] data = new long[data_length];

        for (int i = 0; i < num_blocks; i++) {
            encode_pos = BlockDecoder(encoded_result, i, block_size, block_size, encode_pos, data);
        }

        int remainder = data_length % block_size;

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                data[num_blocks * block_size + i] = bytes2Long(encoded_result, encode_pos, 8);
                encode_pos += 8;
            }
        } else {
            encode_pos = BlockDecoder(encoded_result, num_blocks, block_size, remainder,
                    encode_pos, data);
        }

        return data;
    }

    public static int getDecimalPrecision(String str) {
        // 查找小数点的位置
        int decimalIndex = str.indexOf(".");

        // 如果没有小数点，精度为0
        if (decimalIndex == -1) {
            return 0;
        }

        // 获取小数点后的部分并返回其长度
        return str.substring(decimalIndex + 1).length();
    }

    public static String extractFileName(String path) {
        if (path == null || path.isEmpty()) {
            return "";
        }

        File file = new File(path);
        String fileName = file.getName();

        int dotIndex = fileName.lastIndexOf('.');

        if (dotIndex == -1 || dotIndex == 0) {
            return fileName;
        }

        return fileName.substring(0, dotIndex);
    }

    @Test
    public void testSubcolumn() throws IOException {
//        String parent_dir = "D:/github/xjz17/subcolumn/";
         String parent_dir = "/Users/xiaojinzhao/Documents/GitHub/subcolumn/";

        String input_parent_dir = parent_dir + "dataset/";

        String output_parent_dir = parent_dir + "result/";
        // String output_parent_dir = parent_dir + "result/";

        String outputPath = output_parent_dir + "subcolumn_pso.csv";

        int block_size = 512;

        // int repeatTime = 100;
        int repeatTime = 500;

        // repeatTime = 1;

        CsvWriter writer = new CsvWriter(outputPath, ',', StandardCharsets.UTF_8);
        writer.setRecordDelimiter('\n');

        String[] head = {
                "Dataset",
                "Encoding Algorithm",
                "Encoding Time",
                "Decoding Time",
                "Points",
                "Compressed Size",
                "Compression Ratio"
        };
        writer.writeRecord(head);

        File directory = new File(input_parent_dir);
        // File[] csvFiles = directory.listFiles();
        File[] csvFiles = directory.listFiles((dir, name) -> name.endsWith(".csv"));

        for (File file : csvFiles) {
            String datasetName = extractFileName(file.toString());
            System.out.println(datasetName);

            InputStream inputStream = Files.newInputStream(file.toPath());

            CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
            ArrayList<Double> data1 = new ArrayList<>();

            int max_decimal = 0;
            while (loader.readRecord()) {
                String f_str = loader.getValues()[0];
                if (f_str.isEmpty()) {
                    continue;
                }
                int cur_decimal = getDecimalPrecision(f_str);
                if (cur_decimal > max_decimal) {
                    max_decimal = cur_decimal;
                }
                data1.add(Double.valueOf(f_str));
            }
            inputStream.close();

            if (max_decimal > 17) {
                max_decimal = 17;
            }

            long[] data2_arr = new long[data1.size()];

            long max_mul = (long) Math.pow(10, max_decimal);
            for (int i = 0; i < data1.size(); i++) {
                data2_arr[i] = (long) (data1.get(i) * max_mul);
            }

            // test
            // for (int i = 0; i < data2_arr.length; i++) {
            //     System.out.print(data2_arr[i] + " ");
            // }
            // System.out.println();

            System.out.println(max_decimal);
            byte[] encoded_result = new byte[data2_arr.length * 8];

            long encodeTime = 0;
            long decodeTime = 0;
            double ratio = 0;
            double compressed_size = 0;

            int length = 0;

            long s = System.nanoTime();
            for (int repeat = 0; repeat < repeatTime; repeat++) {
                length = Encoder(data2_arr, block_size, encoded_result);
            }

            long e = System.nanoTime();
            encodeTime += ((e - s) / repeatTime);
            compressed_size += length;

            double ratioTmp;

            ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);

            ratio += ratioTmp;

            System.out.println("Decode");

            long[] data2_arr_decoded = new long[data2_arr.length];

            s = System.nanoTime();

            for (int repeat = 0; repeat < repeatTime; repeat++) {
                data2_arr_decoded = Decoder(encoded_result);
            }

            e = System.nanoTime();
            decodeTime += ((e - s) / repeatTime);

            for (int i = 0; i < data2_arr_decoded.length; i++) {
                assertEquals(data2_arr[i], data2_arr_decoded[i]);
            }

            String[] record = {
                    datasetName,
                    "Sub-columns-PSO",
                    String.valueOf(encodeTime),
                    String.valueOf(decodeTime),
                    String.valueOf(data1.size()),
                    String.valueOf(compressed_size),
                    String.valueOf(ratio)
            };
            writer.writeRecord(record);
            System.out.println(ratio);
        }

        writer.close();
    }

}
