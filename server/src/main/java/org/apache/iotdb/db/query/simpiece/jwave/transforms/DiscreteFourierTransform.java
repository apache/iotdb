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

/**
 * The Discrete Fourier Transform (DFT) is - as the name says - the discrete version of the Fourier
 * Transform applied to a discrete complex valued series. While the DFT can be applied to any
 * complex valued series; of any length, in practice for large series it can take considerable time
 * to compute, while the time taken being proportional to the square of the number on points in the
 * series.
 *
 * @date 25.03.2010 19:56:29
 * @author Christian (graetz23@gmail.com)
 */
import org.apache.iotdb.db.query.simpiece.jwave.datatypes.natives.Complex;
import org.apache.iotdb.db.query.simpiece.jwave.exceptions.JWaveException;
import org.apache.iotdb.db.query.simpiece.jwave.exceptions.JWaveFailure;

/**
 * Discrete Fast Fourier Transform (dFFT)
 *
 * @author Christian (graetz23@gmail.com)
 * @date 14.03.2015 18:30:35
 */
public class DiscreteFourierTransform extends BasicTransform {

  /**
   * Constructor; does nothing
   *
   * @date 25.03.2010 19:56:29
   * @author Christian (graetz23@gmail.com)
   */
  public DiscreteFourierTransform() {

    _name = "Discrete Fourier Transform";
  } // DiscreteFourierTransform

  /**
   * The 1-D forward version of the Discrete Fourier Transform (DFT); The input array arrTime is
   * organized by real and imaginary parts of a complex number using even and odd places for the
   * index. For example: arrTime[ 0 ] = real1, arrTime[ 1 ] = imag1, arrTime[ 2 ] = real2, arrTime[
   * 3 ] = imag2, ... The output arrFreq is organized by the same scheme.
   *
   * @date 25.03.2010 19:56:29
   * @author Christian (graetz23@gmail.com)
   * @throws JWaveException
   * @see jwave.transforms.BasicTransform#forward(double[])
   */
  @Override
  public double[] forward(double[] arrTime) throws JWaveException {

    if (!isBinary(arrTime.length))
      throw new JWaveFailure(
          "DiscreteFourierTransform#forward - "
              + "given array length is not 2^p | p E N "
              + "... = 1, 2, 4, 8, 16, 32, .. "
              + "please use the Ancient Egyptian "
              + "Decomposition for any other array length!");

    int m = arrTime.length;
    double[] arrFreq = new double[m]; // result

    int n = m >> 1; // half of m

    for (int i = 0; i < n; i++) {

      int iR = i * 2;
      int iC = i * 2 + 1;

      arrFreq[iR] = 0.;
      arrFreq[iC] = 0.;

      double arg = -2. * Math.PI * (double) i / (double) n;

      for (int k = 0; k < n; k++) {

        int kR = k * 2;
        int kC = k * 2 + 1;

        double cos = Math.cos(k * arg);
        double sin = Math.sin(k * arg);

        arrFreq[iR] += arrTime[kR] * cos - arrTime[kC] * sin;
        arrFreq[iC] += arrTime[kR] * sin + arrTime[kC] * cos;
      } // k

      arrFreq[iR] /= (double) n;
      arrFreq[iC] /= (double) n;
    } // i

    return arrFreq;
  } // forward

  /**
   * The 1-D reverse version of the Discrete Fourier Transform (DFT); The input array arrFreq is
   * organized by real and imaginary parts of a complex number using even and odd places for the
   * index. For example: arrTime[ 0 ] = real1, arrTime[ 1 ] = imag1, arrTime[ 2 ] = real2, arrTime[
   * 3 ] = imag2, ... The output arrTime is organized by the same scheme.
   *
   * @date 25.03.2010 19:56:29
   * @author Christian (graetz23@gmail.com)
   * @throws JWaveException
   * @see jwave.transforms.BasicTransform#reverse(double[])
   */
  @Override
  public double[] reverse(double[] arrFreq) throws JWaveException {

    if (!isBinary(arrFreq.length))
      throw new JWaveFailure(
          "DiscreteFourierTransform#reverse - "
              + "given array length is not 2^p | p E N "
              + "... = 1, 2, 4, 8, 16, 32, .. "
              + "please use the Ancient Egyptian "
              + "Decomposition for any other array length!");

    int m = arrFreq.length;
    double[] arrTime = new double[m]; // result

    int n = m >> 1; // half of m

    for (int i = 0; i < n; i++) {

      int iR = i * 2;
      int iC = i * 2 + 1;

      arrTime[iR] = 0.;
      arrTime[iC] = 0.;

      double arg = 2. * Math.PI * (double) i / (double) n;

      for (int k = 0; k < n; k++) {

        int kR = k * 2;
        int kC = k * 2 + 1;

        double cos = Math.cos(k * arg);
        double sin = Math.sin(k * arg);

        arrTime[iR] += arrFreq[kR] * cos - arrFreq[kC] * sin;
        arrTime[iC] += arrFreq[kR] * sin + arrFreq[kC] * cos;
      } // k
    } // i

    return arrTime;
  } // reverse

  /*
   * ATTENTION - yet no level implemented; it is ognored!
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 21:18:34 (non-Javadoc)
   * @see jwave.transforms.BasicTransform#forward(double[], int)
   */
  @Override
  public double[] forward(double[] arrTime, int level) throws JWaveException {

    return forward(arrTime);
  } // forward

  /*
   * ATTENTION - yet no level implemented; it is ognored!
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 21:18:34 (non-Javadoc)
   * @see jwave.transforms.BasicTransform#reverse(double[], int)
   */
  @Override
  public double[] reverse(double[] arrFreq, int level) throws JWaveException {

    return reverse(arrFreq);
  } // reverse

  /**
   * The 1-D forward version of the Discrete Fourier Transform (DFT); The input array arrTime is
   * organized by a class called Complex keeping real and imaginary part of a complex number. The
   * output arrFreq is organized by the same scheme.
   *
   * @date 23.11.2010 18:57:34
   * @author Christian (graetz23@gmail.com)
   * @param arrTime array of type Complex keeping coefficients of complex numbers
   * @return array of type Complex keeping the discrete fourier transform coefficients
   */
  public Complex[] forward(Complex[] arrTime) {

    int n = arrTime.length;

    Complex[] arrFreq = new Complex[n]; // result

    for (int i = 0; i < n; i++) {

      arrFreq[i] = new Complex(); // 0. , 0.

      double arg = -2. * Math.PI * (double) i / (double) n;

      for (int k = 0; k < n; k++) {

        double cos = Math.cos(k * arg);
        double sin = Math.sin(k * arg);

        double real = arrTime[k].getReal();
        double imag = arrTime[k].getImag();

        arrFreq[i].addReal(real * cos - imag * sin);
        arrFreq[i].addImag(real * sin + imag * cos);
      } // k

      arrFreq[i].mulReal(1. / (double) n);
      arrFreq[i].mulImag(1. / (double) n);
    } // i

    return arrFreq;
  } // forward

  /**
   * The 1-D reverse version of the Discrete Fourier Transform (DFT); The input array arrFreq is
   * organized by a class called Complex keeping real and imaginary part of a complex number. The
   * output arrTime is organized by the same scheme.
   *
   * @date 23.11.2010 19:02:12
   * @author Christian (graetz23@gmail.com)
   * @param arrFreq array of type Complex keeping the discrete fourier transform coefficients
   * @return array of type Complex keeping coefficients of tiem domain
   */
  public Complex[] reverse(Complex[] arrFreq) {

    int n = arrFreq.length;
    Complex[] arrTime = new Complex[n]; // result

    for (int i = 0; i < n; i++) {

      arrTime[i] = new Complex(); // 0. , 0.

      double arg = 2. * Math.PI * (double) i / (double) n;

      for (int k = 0; k < n; k++) {

        double cos = Math.cos(k * arg);
        double sin = Math.sin(k * arg);

        double real = arrFreq[k].getReal();
        double imag = arrFreq[k].getImag();

        arrTime[i].addReal(real * cos - imag * sin);
        arrTime[i].addImag(real * sin + imag * cos);
      } // k
    } // i

    return arrTime;
  } // reverse
} // class
