/**
 * JWave is distributed under the MIT License (MIT); this file is part of.
 *
 * <p>Copyright (c) 2008-2024 Christian (graetz23@gmail.com)
 *
 * <p>Permission is hereby granted, free of charge, to any person obtaining a copy of this software
 * and associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * <p>The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * <p>THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
 * BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package org.apache.iotdb.db.query.simpiece.jwave.tools;

import org.apache.iotdb.db.query.simpiece.jwave.exceptions.JWaveError;
import org.apache.iotdb.db.query.simpiece.jwave.exceptions.JWaveException;
import org.apache.iotdb.db.query.simpiece.jwave.exceptions.JWaveFailure;

/** @author Christian (graetz23@gmail.com) date Feb 11, 2013 1:42:37 PM */
public class MathToolKit {

  /**
   * Some how useless ~8>
   *
   * @author Christian (graetz23@gmail.com) date Feb 11, 2013 1:42:37 PM
   */
  public MathToolKit() {} // MathToolKit

  /**
   * The method converts a positive integer to the ancient Egyptian multipliers which are actually
   * the multipliers to display the number by a sum of the largest possible powers of two. E.g. 42 =
   * 2^5 + 2^3 + 2^1 = 32 + 8 + 2. However, odd numbers always 2^0 = 1 as the last entry. Also see:
   * http://en.wikipedia.org/wiki/Ancient_Egyptian_multiplication
   *
   * @author Christian (graetz23@gmail.com) date Feb 11, 2013 1:50:42 PM
   * @param number
   * @return
   * @throws JWaveException
   */
  public static int[] decompose(int number) throws JWaveException {

    if (number < 1)
      throw new JWaveFailure("the supported number for decomposition is smaller than one");

    int power = getExponent((double) number);

    int[] tmpArr = new int[power + 1]; // max no of possible multipliers

    int pos = 0;
    double current = (double) number;
    while (current >= 1.) {

      power = getExponent(current);
      tmpArr[pos] = power;
      current = current - scalb(1., power); // 1. * 2 ^ power
      pos++;
    } // while

    int[] ancientEgyptianMultipliers = new int[pos]; // shrink
    for (int c = 0; c < pos; c++) ancientEgyptianMultipliers[c] = tmpArr[c];

    return ancientEgyptianMultipliers;
  } // decompose

  /**
   * splits the given length of the data array to a possible number of blocks in block size and then
   * handles the rest as the ancient egyptian decomposition: e. g. 127 by block size 32 ends up as:
   * 32 | 32 | 32 | 16 | 8 | 4 | 2 | 1.
   *
   * @param number the number that should be decompose; greater than block size
   * @param blockSize the block size as a type of 2^p|p={1,2,4,..} that is first used blocks until a
   *     rest is left; smaller than parameter number.
   * @return an array keeping splits by several time the given block size first and then of a rest
   *     split by the ancient egyptian decomposition.
   * @throws JWaveException if block size is not of type 2^p|p={1,2,4,..}, if block size is smaller
   *     than number or negative input is given.
   */
  public static int[] decompose(int number, int blockSize) throws JWaveException {

    int[] blockedAncientEgyptianMultipliers = null;

    if (!isBinary(blockSize))
      throw new JWaveFailure(
          "given block size is not 2^p|p={1,2,3,4,..}. "
              + "block size shold be e. g.: 4, 8, 16, 32, ..");

    if (number < blockSize)
      throw new JWaveFailure(
          "Given blockSize is greater than the given number " + "to be split by it");

    int noOfBlocks = number % blockSize; // 127 % 32 = 3

    int rest = number - noOfBlocks * blockSize; // 127 - 3 * 32 = 31

    int[] ancientEgyptianMultipliers = decompose(rest);

    int blockedAncientEgyptianMultipliersSize = ancientEgyptianMultipliers.length + noOfBlocks;

    blockedAncientEgyptianMultipliers = new int[blockedAncientEgyptianMultipliersSize];

    int j = 0;
    for (int i = 0; i < blockedAncientEgyptianMultipliersSize; i++)
      if (i < noOfBlocks) blockedAncientEgyptianMultipliers[i] = blockSize;
      else {
        blockedAncientEgyptianMultipliers[i] = ancientEgyptianMultipliers[j];
        j++;
      }

    return blockedAncientEgyptianMultipliers;
  } // decompose

  /**
   * The method converts a list of ancient Egyptian multipliers to the corresponding integer. The
   * ancient Egyptian multipliers are actually the multipliers to display am integer by a sum of the
   * largest possible powers of two. E.g. 42 = 2^5 + 2^3 + 2^1 = 32 + 8 + 2. Also see:
   * http://en.wikipedia.org/wiki/Ancient_Egyptian_multiplication
   *
   * @author Christian (graetz23@gmail.com) date Feb 11, 2013 1:55:54 PM
   * @param ancientEgyptianMultipliers an integer array keeping the ancient Egyptian multipliers
   * @return resulting integer as sum of powers of two
   * @throws JWaveException
   */
  public static int compose(int[] ancientEgyptianMultipliers) throws JWaveException {

    if (ancientEgyptianMultipliers == null) throw new JWaveError("given array is null");

    int number = 0;

    int noOfAncientEgyptianMultipliers = ancientEgyptianMultipliers.length;
    for (int m = 0; m < noOfAncientEgyptianMultipliers; m++) {

      int ancientEgyptianMultiplier = ancientEgyptianMultipliers[m];

      number += (int) scalb(1., ancientEgyptianMultiplier); // 1. * 2^p
    } // compose

    return number;
  }

  /**
   * Checks if given number is of type 2^p = 1, 2, 4, 8, 18, 32, 64, .., 1024, ..
   *
   * @author Christian (graetz23@gmail.com) 10.02.2014 20:18:26
   * @param number any positive integer
   * @return true if is 2^p else false
   */
  public static boolean isBinary(int number) {
    // DLM#23 Power of 2 test
    return number > 0 && ((number & (number - 1)) == 0);
  } // isBinary

  /**
   * Replaced Math.getExponent due to google's Android OS is not supporting it in Math library.
   *
   * @author Christian (graetz23@gmail.com) date Feb 11, 2013 1:47:05 PM
   * @author sashi
   * @date 19.04.2011 15:43:16
   * @param f
   * @return p of 2^p <= f < 2^(p+1)
   */
  public static int getExponent(double f) {

    int exp = (int) (Math.log(f) / Math.log(2.));

    return exp;
  } // exp

  /**
   * Replaced Math.scalb due to google's Android OS is not supporting it in Math library.
   *
   * @author Christian (graetz23@gmail.com) date Feb 11, 2013 1:46:33 PM
   * @param f
   * @param scaleFactor
   * @return f times 2^(scaleFactor)
   */
  public static double scalb(double f, int scaleFactor) {

    double res = f * Math.pow(2., scaleFactor);

    return res;
  } // scalb

  /**
   * Returns a sampled array of sine waves for given number of oscillations.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 14.03.2015 19:38:39
   * @param samplingRate should be great than 2 and likely to be of 2^p | p E N
   * @param noOfOscillations should be of natural numbers except zero
   * @return sampled array keeping a number of sine waves
   */
  public static double[] createSineOscillation(int samplingRate, int noOfOscillations) {

    if (samplingRate < 1) samplingRate = 2;

    if (noOfOscillations < 1) noOfOscillations = 1;

    double[] arrTime = new double[samplingRate];

    for (int i = 0; i < samplingRate; i++) {
      double arg = 2. * Math.PI * (double) i / (double) samplingRate * (double) noOfOscillations;
      double sine = Math.sin(arg);
      arrTime[i] = sine;
    } // i

    return arrTime;
  } // createSineOscillation

  /**
   * Returns a sampled array of cosine waves for given number of oscillations.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 14.03.2015 19:41:16
   * @param samplingRate should be great than 2 and likely to be of 2^p | p E N
   * @param noOfOscillations should be of natural numbers except zero
   * @return sampled array keeping a number of cosine waves
   */
  public static double[] createCosineOscillation(int samplingRate, int noOfOscillations) {

    if (samplingRate < 1) samplingRate = 2;

    if (noOfOscillations < 1) noOfOscillations = 1;

    double[] arrTime = new double[samplingRate];

    for (int i = 0; i < samplingRate; i++) {
      double arg = 2. * Math.PI * (double) i / (double) samplingRate * (double) noOfOscillations;
      double sine = Math.cos(arg);
      arrTime[i] = sine;
    } // i

    return arrTime;
  } // createCoSineOscillation
} // class
