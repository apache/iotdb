package org.apache.iotdb.tool.ui.config;

/**
 * TableAlign
 *
 * <p>table align [ top-left | top-center | top-right | center-left | center | center-right
 * bottom-left | bottom-center | bottom-right | baseline-left | baseline-center | baseline-right ]
 *
 * @author shenguanchu
 */
public enum TableAlign {
  /** center */
  CENTER("CENTER"),
  /** center-left */
  CENTER_LEFT("center-left");

  String align;

  TableAlign(String align) {
    this.align = align;
  }

  public String getAlign() {
    return align;
  }

  public void setAlign(String align) {
    this.align = align;
  }
}
