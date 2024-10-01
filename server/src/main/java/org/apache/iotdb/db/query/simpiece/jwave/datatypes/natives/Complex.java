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
package org.apache.iotdb.db.query.simpiece.jwave.datatypes.natives;

/**
 * A class to represent a complex number. A Complex object is immutable once created; the add,
 * subtract and multiply routines return newly-created Complex objects containing each requested
 * result.
 *
 * @date 19.11.2010 13:20:48
 * @author Christian (graetz23@gmail.com)
 */
public class Complex {

  /** The real number. */
  private double _r;

  /** The imaginary number. */
  private double _j;

  /**
   * Standard constructor.
   *
   * @date 19.11.2010 13:38:56
   * @author Christian (graetz23@gmail.com)
   */
  public Complex() {
    _r = 0.;
    _j = 0.;
  } // Complex

  /**
   * Copy constructor.
   *
   * @date 19.11.2010 13:22:54
   * @author Christian (graetz23@gmail.com)
   * @param c complex number
   */
  public Complex(Complex c) {
    _r = c._r;
    _j = c._j;
  } // Complex

  /**
   * Constructor taking real and imaginary number.
   *
   * @date 19.11.2010 13:21:48
   * @author Christian (graetz23@gmail.com)
   * @param r real number
   * @param j imaginary number
   */
  public Complex(double r, double j) {
    _r = r;
    _j = j;
  } // Complex

  /**
   * Display the current Complex as a String, for usage in println( ) or writing the complex into a
   * file.
   *
   * @date 19.11.2010 13:23:13
   * @author Christian (graetz23@gmail.com)
   */
  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer().append(_r);
    if (_j >= 0) sb.append('+');
    else sb.append('-');
    return sb.append(_j).append('j').toString();
  } // toString

  /**
   * Return the real number.
   *
   * @date 19.11.2010 13:23:34
   * @author Christian (graetz23@gmail.com)
   * @return real number of this complex number
   */
  public double getReal() {
    return _r;
  } // getReal( )

  /**
   * Return the imaginary number.
   *
   * @date 19.11.2010 13:23:51
   * @author Christian (graetz23@gmail.com)
   * @return imaginary number of this complex number
   */
  public double getImag() {
    return _j;
  } // getImag

  /**
   * Set the real number.
   *
   * @date 23.11.2010 18:44:52
   * @author Christian (graetz23@gmail.com)
   * @param r the real number
   */
  public void setReal(double r) {
    _r = r;
  } // setReal

  /**
   * Set the imaginary number.
   *
   * @date 23.11.2010 18:45:16
   * @author Christian (graetz23@gmail.com)
   * @param j the imaginary number
   */
  public void setImag(double j) {
    _j = j;
  } // setImag

  /**
   * Add to real number.
   *
   * @date 23.11.2010 18:49:57
   * @author Christian (graetz23@gmail.com)
   * @param r the real number
   */
  public void addReal(double r) {
    _r += r;
  } // addReal

  /**
   * Add to imaginary number.
   *
   * @date 23.11.2010 18:50:23
   * @author Christian (graetz23@gmail.com)
   * @param j the imaginary number
   */
  public void addImag(double j) {
    _j += j;
  } // addImag

  /**
   * multiply scalar to real number.
   *
   * @date 23.11.2010 18:53:27
   * @author Christian (graetz23@gmail.com)
   * @param s scalar
   */
  public void mulReal(double s) {
    _r *= s;
  } // mulReal

  /**
   * multiply scalar to imaginary number.
   *
   * @date 23.11.2010 18:54:48
   * @author Christian (graetz23@gmail.com)
   * @param s scalar
   */
  public void mulImag(double s) {
    _j *= s;
  } // mulImag

  /**
   * Calculate the magnitude of the complex number.
   *
   * @date 19.11.2010 13:24:28
   * @author Christian (graetz23@gmail.com)
   * @return magnitude of this complex number
   */
  public double getMag() {
    return Math.sqrt(_r * _r + _j * _j);
  } // getMag( )

  /**
   * Calculates the angle phi of a complex number.
   *
   * @date 19.11.2010 13:24:48
   * @author Christian (graetz23@gmail.com)
   * @return angle of this complex number
   */
  public double getPhi() {
    if (_r == 0. && _j == 0) return 0.;
    double phi = Math.toDegrees(Math.atan(Math.abs(_j / _r)));
    if (_r >= 0. && _j >= 0.) // 1. quadrant
    return phi;
    if (_r <= 0. && _j >= 0.) // 2. quadrant
    return 180. - phi;
    if (_r <= 0. && _j <= 0.) // 3. quadrant
    return phi + 180.;
    if (_r >= 0. && _j <= 0.) // 4. quadrant
    return 360. - phi;
    return Math.toDegrees(Math.atan(Math.abs(_j / _r)));
  } // getPhi( )

  /**
   * Returns the stored values as new double array: [ real, imag ].
   *
   * @date 19.11.2010 13:25:38
   * @author Christian (graetz23@gmail.com)
   * @return returns stored values as array [ real, imag ]
   */
  public double[] toArr() {
    double[] arr = {_r, _j};
    return arr;
  } // toArr

  /**
   * Returns the conjugate complex number of this complex number.
   *
   * @date 19.11.2010 19:36:52
   * @author Thomas Leduc
   * @return new object of Complex keeping the result
   */
  public Complex conjugate() {
    return new Complex(_r, -_j);
  } // conjugate

  /**
   * Add another complex number to this one and return.
   *
   * @date 19.11.2010 13:25:55
   * @author Christian (graetz23@gmail.com)
   * @param c complex number
   * @return new object of Complex keeping the result
   */
  public Complex add(Complex c) {
    return new Complex(_r + c._r, _j + c._j);
  } // add

  /**
   * Subtract another complex number from this one.
   *
   * @date 19.11.2010 13:27:05
   * @author Christian (graetz23@gmail.com)
   * @param c complex number
   * @return new object of Complex keeping the result
   */
  public Complex sub(Complex c) {
    return new Complex(_r - c._r, _j - c._j);
  } // sub

  /**
   * Multiply this complex number times another one.
   *
   * @date 19.11.2010 13:27:36
   * @author Christian (graetz23@gmail.com)
   * @param c complex number
   * @return new object of Complex keeping the result
   */
  public Complex mul(Complex c) {
    return new Complex(_r * c._r - _j * c._j, _r * c._j + _j * c._r);
  } // mul

  /**
   * Multiply this complex number times a scalar.
   *
   * @date 19.11.2010 13:28:03
   * @author Christian (graetz23@gmail.com)
   * @param s scalar
   * @return new object of Complex keeping the result
   */
  public Complex mul(double s) {
    return new Complex(_r * s, _j * s);
  } // mul

  /**
   * Divide this complex number by another one.
   *
   * @date 19.11.2010 19:45:02
   * @author Thomas Leduc
   * @param c complex number
   * @return new object of Complex keeping the result
   */
  public Complex div(Complex c) {
    return mul(c.conjugate()).div(c._r * c._r + c._j * c._j);
  } // div

  /**
   * Divide this complex number by a scalar.
   *
   * @date 19.11.2010 13:29:49
   * @author Thomas Leduc
   * @param s scalar
   * @return new object of Complex keeping the result
   */
  public Complex div(double s) {
    return mul(1. / s);
  } // div

  /**
   * Generates a hash code for this object.
   *
   * @date 19.11.2010 19:42:39
   * @author Thomas Leduc
   * @see Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    long temp;
    temp = Double.doubleToLongBits(_j);
    result = prime * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(_r);
    result = prime * result + (int) (temp ^ (temp >>> 32));
    return result;
  } // hashCode

  /**
   * Compare this Complex number with another one.
   *
   * @date 19.11.2010 13:30:35
   * @author Thomas Leduc
   * @see Object#equals(Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    Complex other = (Complex) obj;
    if (Double.doubleToLongBits(_j) != Double.doubleToLongBits(other._j)) return false;
    if (Double.doubleToLongBits(_r) != Double.doubleToLongBits(other._r)) return false;
    return true;
  } // equals

  /**
   * Print this complex number to console.
   *
   * @date 19.11.2010 13:31:16
   * @author Christian (graetz23@gmail.com)
   */
  public void show() {
    if (_j < 0) System.out.println(getReal() + " - j" + Math.abs(getImag()));
    else System.out.println(getReal() + " + j" + getImag());
  } // show

  /**
   * Print this complex number to console with an identifier before.
   *
   * @date 19.11.2010 13:31:32
   * @author Christian (graetz23@gmail.com)
   * @param ident string to label this complex number
   */
  public void show(String ident) {
    if (_j < 0) System.out.println(ident + ": " + getReal() + " - j" + Math.abs(getImag()));
    else System.out.println(ident + ": " + getReal() + " + j" + getImag());
  } // show

  /**
   * Print magnitude to console out.
   *
   * @date 19.11.2010 13:32:15
   * @author Christian (graetz23@gmail.com)
   */
  public void showMag() {
    System.out.println(getMag());
  } // showMag

  /**
   * Print angle to console out.
   *
   * @date 19.11.2010 13:32:33
   * @author Christian (graetz23@gmail.com)
   */
  public void showPhi() {
    System.out.println(getPhi());
  } // showPhi
} // class
