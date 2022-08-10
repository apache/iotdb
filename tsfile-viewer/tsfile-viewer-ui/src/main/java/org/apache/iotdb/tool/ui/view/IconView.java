package org.apache.iotdb.tool.ui.view;

import javafx.scene.image.Image;
import javafx.scene.image.ImageView;

/**
 * ImageView extension for icons.
 *
 * @author shenguanchu
 */
public class IconView extends ImageView {
  private static final int DEFAULT_ICON_SIZE = 16;

  public IconView() {}

  /** @param path Path to resource. */
  public IconView(String path) {
    this(new Image(path));
  }

  /** @param image Image resource. */
  public IconView(Image image) {
    super(image);
    fitHeightProperty().set(DEFAULT_ICON_SIZE);
    fitWidthProperty().set(DEFAULT_ICON_SIZE);
  }

  /**
   * @param image Image resource.
   * @param size Image width/height.
   */
  public IconView(Image image, int size) {
    super(image);
    fitHeightProperty().set(size);
    fitWidthProperty().set(size);
  }
}
