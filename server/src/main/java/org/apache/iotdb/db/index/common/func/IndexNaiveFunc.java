package org.apache.iotdb.db.index.common.func;

import java.io.IOException;

/**
 * Do something without input and output.
 *
 * <p>This is a <a href="package-summary.html">functional interface</a> whose functional method is
 * {@link #act()}.
 */
@FunctionalInterface
public interface IndexNaiveFunc {

  /** Do something. */
  void act() throws IOException;
}
