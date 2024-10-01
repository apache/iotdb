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
package org.apache.iotdb.db.query.simpiece.jwave.transforms;

import org.apache.iotdb.db.query.simpiece.jwave.datatypes.natives.Complex;
import org.apache.iotdb.db.query.simpiece.jwave.exceptions.JWaveError;
import org.apache.iotdb.db.query.simpiece.jwave.exceptions.JWaveException;
import org.apache.iotdb.db.query.simpiece.jwave.exceptions.JWaveFailure;
import org.apache.iotdb.db.query.simpiece.jwave.tools.MathToolKit;
import org.apache.iotdb.db.query.simpiece.jwave.transforms.wavelets.Wavelet;

/**
 * Basic Wave for transformations like Fast Fourier Transform (FFT), Fast Wavelet Transform (FWT),
 * Fast Wavelet Packet Transform (WPT), or Discrete Wavelet Transform (DWT). Naming of this class
 * due to en.wikipedia.org; to write Fourier series in terms of the 'basic waves' of function:
 * e^(2*pi*i*w).
 *
 * @date 08.02.2010 11:11:59
 * @author Christian (graetz23@gmail.com)
 */
public abstract class BasicTransform {

  /**
   * String identifier of the current Transform object.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 14.03.2015 14:25:56
   */
  protected String _name;

  /**
   * @author Christian (graetz23@gmail.com)
   * @date 19.02.2014 18:38:21
   */
  public BasicTransform() {

    _name = null;
  } // BasicTransform

  /**
   * Returns String identifier of current type of BasicTransform Object.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 14.03.2015 18:13:34
   * @return identifier as String
   */
  public String getName() {
    return _name;
  } // getName

  /**
   * Returns the stored Wavelet object or null pointer.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 14.03.2015 18:26:44
   * @return object of type Wavelet of null pointer
   * @throws JWaveFailure if Wavelet object is not available
   */
  public Wavelet getWavelet() throws JWaveFailure {
    throw new JWaveFailure("BasicTransform#getWavelet - not available");
  } // getWavelet

  /**
   * Performs the forward transform from time domain to frequency or Hilbert domain for a given
   * array depending on the used transform algorithm by inheritance.
   *
   * @date 10.02.2010 08:23:24
   * @author Christian (graetz23@gmail.com)
   * @param arrTime coefficients of 1-D time domain
   * @return coefficients of 1-D frequency or Hilbert space
   * @throws JWaveException
   */
  public abstract double[] forward(double[] arrTime) throws JWaveException;

  /**
   * Performs the reverse transform from frequency or Hilbert domain to time domain for a given
   * array depending on the used transform algorithm by inheritance.
   *
   * @date 10.02.2010 08:23:24
   * @author Christian (graetz23@gmail.com)
   * @param arrFreq coefficients of 1-D frequency or Hilbert domain
   * @return coefficients of time series of 1-D frequency or Hilbert space
   * @throws JWaveException
   */
  public abstract double[] reverse(double[] arrFreq) throws JWaveException;

  /**
   * Performs the forward transform from time domain to Hilbert domain of a given level depending on
   * the used transform algorithm by inheritance.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 22.03.2015 11:33:11
   * @param arrTime
   * @param level the level of Hilbert space; energy & detail coefficients
   * @return array keeping Hilbert space of requested level
   * @throws JWaveException if given array is not of type 2^p | p E N or given level does not match
   *     the possibilities of given array.
   */
  public double[] forward(double[] arrTime, int level) throws JWaveException {

    throw new JWaveError(
        "BasicTransform#forward - " + "method is not implemented for this transform type!");
  } // forward

  /**
   * Performs the reverse transform from Hilbert domain of a given level to time domain depending on
   * the used transform algorithm by inheritance.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 22.03.2015 11:34:27
   * @param arrFreq
   * @param level the level of Hilbert space; energy & detail coefficients
   * @return array keeping Hilbert space of requested level
   * @throws JWaveException if given array is not of type 2^p | p E N or given level does not match
   *     the possibilities of given array.
   */
  public double[] reverse(double[] arrFreq, int level) throws JWaveException {

    throw new JWaveError(
        "BasicTransform#reverse - " + "method is not implemented for this transform type!");
  } // reverse

  /**
   * Generates from a 2-D decomposition a 1-D time series.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 17.08.2014 10:07:19
   * @param matDeComp 2-D Hilbert spaces: [ 0 .. p ][ 0 .. N ] where p is the exponent of N=2^p
   * @return a 1-D time domain signal
   * @throws JWaveException
   */
  public double[][] decompose(double[] arrTime) throws JWaveException {

    throw new JWaveError(
        "BasicTransform#decompose - " + "method is not implemented for this transform type!");
  } // decompose

  /**
   * Generates from a 1-D signal a 2-D output, where the second dimension are the levels of the
   * wavelet transform. The first level should keep the original coefficients. All following levels
   * should keep each step of the decomposition of the Fast Wavelet Transform. However, each level
   * of the this decomposition matrix is having the full set, full energy and full details, that are
   * needed to do a full reconstruction. So one can select a level filter it and then do
   * reconstruction only from this single line! BY THIS METHOD, THE _HIGHEST_ LEVEL IS _ALWAYS_
   * TAKEN FOR RECONSTRUCTION!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 17.08.2014 10:07:19
   * @param matDeComp 2-D Hilbert spaces: [ 0 .. p ][ 0 .. M ] where p is the exponent of M=2^p |
   *     pEN
   * @return a 1-D time domain signal
   */
  public double[] recompose(double[][] matDeComp) throws JWaveException {

    // Each level of the matrix is having the full set (full energy + details)
    // of decomposition. Therefore, each level can be used to do a full reconstruction,
    int level = matDeComp.length - 1; // selected highest level in general.
    double[] arrTime = null;
    try {
      arrTime = recompose(matDeComp, level);
    } catch (JWaveFailure e) {
      e.showMessage();
      e.printStackTrace();
    } // try

    return arrTime;
  } // recompose

  /**
   * Generates from a 1-D signal a 2-D output, where the second dimension are the levels of the
   * wavelet transform. The first level should keep the original coefficients. All following levels
   * should keep each step of the decomposition of the Fast Wavelet Transform. However, each level
   * of the this decomposition matrix is having the full set, full energy and full details, that are
   * needed to do a full reconstruction. So one can select a level filter it and then do
   * reconstruction only from this single line!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 22.03.2015 15:12:19
   * @param matDeComp 2-D Hilbert spaces: [ 0 .. p ][ 0 .. M ] where p is the exponent of M=2^p |
   *     pEN
   * @param level the level that should be used for reconstruction
   * @return the reconstructed time series of a selected level
   * @throws JWaveException
   */
  public double[] recompose(double[][] matDeComp, int level) throws JWaveException {

    double[] arrTime = null;
    try {
      arrTime = recompose(matDeComp, level);
    } catch (JWaveFailure e) {
      e.showMessage();
      e.printStackTrace();
    } // try

    return arrTime;
  } // recompose

  /**
   * Performs the forward transform from time domain to frequency or Hilbert domain for a given
   * array depending on the used transform algorithm by inheritance.
   *
   * @date 16.02.2014 14:42:57
   * @author Christian (graetz23@gmail.com)
   * @param arrTime coefficients of 1-D time domain
   * @return coefficients of 1-D frequency or Hilbert domain
   * @throws JWaveException
   */
  public Complex[] forward(Complex[] arrTime) throws JWaveException {

    double[] arrTimeBulk = new double[2 * arrTime.length];

    for (int i = 0; i < arrTime.length; i++) {

      // TODO rehack complex number splitting this to: { r1, r2, r3, .., c1, c2, c3, .. }
      int k = i * 2;
      arrTimeBulk[k] = arrTime[i].getReal();
      arrTimeBulk[k + 1] = arrTime[i].getImag();
    } // i blown to k = 2 * i

    double[] arrHilbBulk = forward(arrTimeBulk);

    Complex[] arrHilb = new Complex[arrTime.length];

    for (int i = 0; i < arrTime.length; i++) {

      int k = i * 2;
      arrHilb[i] = new Complex(arrHilbBulk[k], arrHilbBulk[k + 1]);
    } // k = 2 * i shrink to i

    return arrHilb;
  } // forward

  /**
   * Performs the reverse transform from frequency or Hilbert domain to time domain for a given
   * array depending on the used transform algorithm by inheritance.
   *
   * @date 16.02.2014 14:42:57
   * @author Christian (graetz23@gmail.com)
   * @param arrFreq coefficients of 1-D frequency or Hilbert domain
   * @return coefficients of 1-D time domain
   * @throws JWaveException
   */
  public Complex[] reverse(Complex[] arrHilb) throws JWaveException {

    double[] arrHilbBulk = new double[2 * arrHilb.length];

    for (int i = 0; i < arrHilb.length; i++) {

      int k = i * 2;
      arrHilbBulk[k] = arrHilb[i].getReal();
      arrHilbBulk[k + 1] = arrHilb[i].getImag();
    } // i blown to k = 2 * i

    double[] arrTimeBulk = reverse(arrHilbBulk);

    Complex[] arrTime = new Complex[arrHilb.length];

    for (int i = 0; i < arrTime.length; i++) {

      int k = i * 2;
      arrTime[i] = new Complex(arrTimeBulk[k], arrTimeBulk[k + 1]);
    } // k = 2 * i shrink to i

    return arrTime;
  } // reverse

  /**
   * Performs the 2-D forward transform from time domain to frequency or Hilbert domain for a given
   * matrix depending on the used transform algorithm by inheritance.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 22.03.2015 12:47:01
   * @param matTime
   * @return
   * @throws JWaveException if matrix id not of matching dimension like 2^p | pEN
   */
  public double[][] forward(double[][] matTime) throws JWaveException {

    int maxM = MathToolKit.getExponent(matTime.length);
    int maxN = MathToolKit.getExponent(matTime[0].length);
    return forward(matTime, maxM, maxN);
  } // forward

  /**
   * Performs the 2-D forward transform from time domain to frequency or Hilbert domain of a certain
   * level for a given matrix depending on the used transform algorithm by inheritance. The
   * supported level has to match the possible dimensions of the given matrix.
   *
   * @date 10.02.2010 11:00:29
   * @author Christian (graetz23@gmail.com)
   * @param matTime coefficients of 2-D time domain
   * @param lvlM level to stop in dimension M of the matrix
   * @param lvlN level to stop in dimension N of the matrix
   * @return coefficients of 2-D frequency or Hilbert domain
   * @throws JWaveException
   */
  public double[][] forward(double[][] matTime, int lvlM, int lvlN) throws JWaveException {

    int noOfRows = matTime.length;
    int noOfCols = matTime[0].length;

    double[][] matHilb = new double[noOfRows][noOfCols];

    for (int i = 0; i < noOfRows; i++) {

      double[] arrTime = new double[noOfCols];

      for (int j = 0; j < noOfCols; j++) arrTime[j] = matTime[i][j];

      double[] arrHilb = forward(arrTime, lvlN);

      for (int j = 0; j < noOfCols; j++) matHilb[i][j] = arrHilb[j];
    } // rows

    for (int j = 0; j < noOfCols; j++) {

      double[] arrTime = new double[noOfRows];

      for (int i = 0; i < noOfRows; i++) arrTime[i] = matHilb[i][j];

      double[] arrHilb = forward(arrTime, lvlM);

      for (int i = 0; i < noOfRows; i++) matHilb[i][j] = arrHilb[i];
    } // cols

    return matHilb;
  } // forward

  /**
   * Performs the 2-D reverse transform from frequency or Hilbert or time domain to time domain for
   * a given matrix depending on the used transform algorithm by inheritance.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 10.02.2010 11:01:38
   * @param matFreq
   * @return
   * @throws JWaveException
   */
  public double[][] reverse(double[][] matFreq) throws JWaveException {

    int maxM = MathToolKit.getExponent(matFreq.length);
    int maxN = MathToolKit.getExponent(matFreq[0].length);
    return reverse(matFreq, maxM, maxN);
  } // reverse

  /**
   * Performs the 2-D reverse transform from frequency or Hilbert or time domain to time domain of a
   * certain level for a given matrix depending on the used transform algorithm by inheritance.
   *
   * @date 22.03.2015 12:49:16
   * @author Christian (graetz23@gmail.com)
   * @param matFreq coefficients of 2-D frequency or Hilbert domain
   * @param lvlM level to start reconstruction for dimension M of the matrix
   * @param lvlN level to start reconstruction for dimension N of the matrix
   * @return coefficients of 2-D time domain
   * @throws JWaveException
   */
  public double[][] reverse(double[][] matFreq, int lvlM, int lvlN) throws JWaveException {

    int noOfRows = matFreq.length;
    int noOfCols = matFreq[0].length;

    double[][] matTime = new double[noOfRows][noOfCols];

    for (int j = 0; j < noOfCols; j++) {

      double[] arrFreq = new double[noOfRows];

      for (int i = 0; i < noOfRows; i++) arrFreq[i] = matFreq[i][j];

      double[] arrTime = reverse(arrFreq, lvlM); // AED

      for (int i = 0; i < noOfRows; i++) matTime[i][j] = arrTime[i];
    } // cols

    for (int i = 0; i < noOfRows; i++) {

      double[] arrFreq = new double[noOfCols];

      for (int j = 0; j < noOfCols; j++) arrFreq[j] = matTime[i][j];

      double[] arrTime = reverse(arrFreq, lvlN); // AED

      for (int j = 0; j < noOfCols; j++) matTime[i][j] = arrTime[j];
    } // rows

    return matTime;
  } // reverse

  /**
   * Performs the 3-D forward transform from time domain to frequency or Hilbert domain for a given
   * space (3-D) depending on the used transform algorithm by inheritance.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 10.07.2010 18:08:17
   * @param spcTime
   * @return
   * @throws JWaveException
   */
  public double[][][] forward(double[][][] spcTime) throws JWaveException {

    int maxP = MathToolKit.getExponent(spcTime.length);
    int maxQ = MathToolKit.getExponent(spcTime[0].length);
    int maxR = MathToolKit.getExponent(spcTime[0][0].length);
    return forward(spcTime, maxP, maxQ, maxR);
  } // forward

  /**
   * Performs the 3-D forward transform from time domain to frequency or Hilbert domain of a certain
   * level for a given space (3-D) depending on the used transform algorithm by inheritance.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 22.03.2015 12:58:34
   * @param spcTime coefficients of 3-D time domain domain
   * @return coefficients of 3-D frequency or Hilbert domain
   * @throws JWaveException
   */
  public double[][][] forward(double[][][] spcTime, int lvlP, int lvlQ, int lvlR)
      throws JWaveException {

    int noOfRows = spcTime.length; // first dimension
    int noOfCols = spcTime[0].length; // second dimension
    int noOfHigh = spcTime[0][0].length; // third dimension

    double[][][] spcHilb = new double[noOfRows][noOfCols][noOfHigh];

    for (int i = 0; i < noOfRows; i++) {

      double[][] matTime = new double[noOfCols][noOfHigh];

      for (int j = 0; j < noOfCols; j++) {

        for (int k = 0; k < noOfHigh; k++) {

          matTime[j][k] = spcTime[i][j][k];
        } // high
      } // cols

      double[][] matHilb = forward(matTime, lvlP, lvlQ); // 2-D forward

      for (int j = 0; j < noOfCols; j++) {

        for (int k = 0; k < noOfHigh; k++) {

          spcHilb[i][j][k] = matHilb[j][k];
        } // high
      } // cols
    } // rows

    for (int j = 0; j < noOfCols; j++) {

      for (int k = 0; k < noOfHigh; k++) {

        double[] arrTime = new double[noOfRows];

        for (int i = 0; i < noOfRows; i++) arrTime[i] = spcHilb[i][j][k];

        double[] arrHilb = forward(arrTime, lvlR); // 1-D forward

        for (int i = 0; i < noOfRows; i++) spcHilb[i][j][k] = arrHilb[i];
      } // high
    } // cols

    return spcHilb;
  } // forward

  /**
   * Performs the 3-D reverse transform from frequency or Hilbert domain to time domain for a given
   * space (3-D) depending on the used transform algorithm by inheritance.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 10.07.2010 18:09:54
   * @param spcHilb
   * @return
   * @throws JWaveException
   */
  public double[][][] reverse(double[][][] spcHilb) throws JWaveException {

    int maxP = MathToolKit.getExponent(spcHilb.length);
    int maxQ = MathToolKit.getExponent(spcHilb[0].length);
    int maxR = MathToolKit.getExponent(spcHilb[0][0].length);
    return reverse(spcHilb, maxP, maxQ, maxR);
  } // reverse

  /**
   * Performs the 3-D reverse transform from frequency or Hilbert domain of a certain level to time
   * domain for a given space (3-D) depending on the used transform algorithm by inheritance. The
   * supported coefficients have to match the level of Hilbert space.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 22.03.2015 13:01:47
   * @param spcHilb coefficients of 3-D frequency or Hilbert domain
   * @return coefficients of 3-D time domain
   * @throws JWaveException
   */
  public double[][][] reverse(double[][][] spcHilb, int lvlP, int lvlQ, int lvlR)
      throws JWaveException {

    int noOfRows = spcHilb.length; // first dimension
    int noOfCols = spcHilb[0].length; // second dimension
    int noOfHigh = spcHilb[0][0].length; // third dimension

    double[][][] spcTime = new double[noOfRows][noOfCols][noOfHigh];

    for (int i = 0; i < noOfRows; i++) {

      double[][] matHilb = new double[noOfCols][noOfHigh];

      for (int j = 0; j < noOfCols; j++) {

        for (int k = 0; k < noOfHigh; k++) {

          matHilb[j][k] = spcHilb[i][j][k];
        } // high
      } // cols

      double[][] matTime = reverse(matHilb, lvlP, lvlQ); // 2-D reverse

      for (int j = 0; j < noOfCols; j++) {

        for (int k = 0; k < noOfHigh; k++) {

          spcTime[i][j][k] = matTime[j][k];
        } // high
      } // cols
    } // rows

    for (int j = 0; j < noOfCols; j++) {

      for (int k = 0; k < noOfHigh; k++) {

        double[] arrHilb = new double[noOfRows];

        for (int i = 0; i < noOfRows; i++) arrHilb[i] = spcTime[i][j][k];

        double[] arrTime = reverse(arrHilb, lvlR); // 1-D reverse

        for (int i = 0; i < noOfRows; i++) spcTime[i][j][k] = arrTime[i];
      } // high
    } // cols

    return spcTime;
  } // reverse

  /**
   * Returns true if given integer is of type binary (2, 4, 8, 16, ..) else the method returns
   * false.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 22.03.2015 13:31:39
   * @param number an integer of type 2, 4, 8, 16, 32, 64, ...
   * @return true if number is a binary number else false
   */
  protected boolean isBinary(int number) {

    return MathToolKit.isBinary(number); // use MathToolKit or implement
  } // isBinary

  /**
   * Return the exponent of a binary a number
   *
   * @author Christian (graetz23@gmail.com)
   * @date 22.03.2015 13:35:50
   * @param number any integer that fulfills 2^p | pEN
   * @return p as number = 2^p | pEN
   * @throws JWaveException if given number is not a binary number
   */
  protected int calcExponent(int number) throws JWaveException {

    if (!isBinary(number))
      throw new JWaveFailure(
          "BasicTransform#calcExponent - "
              + "given number is not binary: "
              + "2^p | pEN .. = 1, 2, 4, 8, 16, 32, .. ");

    return (int) (MathToolKit.getExponent((int) (number))); // use MathToolKit or implement
  } // calcExponent
} // BasicTransform
