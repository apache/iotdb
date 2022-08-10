package org.apache.iotdb.tool.ui.scene;

import javafx.scene.Parent;
import javafx.scene.Scene;

/**
 * 使用page替换scene概念
 *
 * @author oortCloudFei
 */
public abstract class Page<P extends Parent> {

  protected P root;

  private Scene scene;

  private double width;

  private double height;

  public Page(P root, double width, double height) {
    this.root = root;
    this.width = width;
    this.height = height;
    this.scene = new Scene(root, width, height);
  }

  public String getName() {
    return this.getClass().getSimpleName();
  }

  public double getWidth() {
    return width;
  }

  public double getHeight() {
    return height;
  }

  public Scene getScene() {
    return scene;
  }

  public P getRoot() {
    return root;
  }
}
