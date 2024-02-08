package org.apache.iotdb.tsfile.file.metadata.statistics.util;

public class Complex {
  private final double real;
  private final double imag;

  public Complex(double real, double imag) {
    this.real = real;
    this.imag = imag;
  }

  public Complex add(Complex other) {
    return new Complex(this.real + other.real, this.imag + other.imag);
  }

  public Complex subtract(Complex other) {
    return new Complex(this.real - other.real, this.imag - other.imag);
  }

  public Complex multiply(Complex other) {
    double realPart = this.real * other.real - this.imag * other.imag;
    double imagPart = this.real * other.imag + this.imag * other.real;
    return new Complex(realPart, imagPart);
  }

  public Complex conjugate() {
    return new Complex(this.real, -this.imag);
  }

  public static Complex[] conjugate(Complex[] a) {
    Complex[] b = new Complex[a.length];
    for (int i = 0; i < a.length; i++) b[i] = a[i].conjugate();
    return b;
  }

  public static Complex[] multiply(Complex[] a, Complex[] b) {
    Complex[] c = new Complex[a.length];
    for (int i = 0; i < a.length; i++) c[i] = a[i].multiply(b[i]);
    return c;
  }

  public Complex scale(double factor) {
    return new Complex(this.real * factor, this.imag * factor);
  }

  @Override
  public String toString() {
    return "(" + real + " + " + imag + "i)";
  }

  public double getReal() {
    return this.real;
  }

  public double getImag() {
    return this.imag;
  }
}
