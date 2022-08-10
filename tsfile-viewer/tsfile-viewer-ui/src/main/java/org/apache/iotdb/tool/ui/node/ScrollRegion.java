// package org.apache.iotdb.tool.ui.node;
//
// import javafx.beans.value.ObservableValue;
// import javafx.collections.ObservableList;
// import javafx.geometry.Bounds;
// import javafx.geometry.Orientation;
// import javafx.scene.Group;
// import javafx.scene.Node;
// import javafx.scene.control.ScrollBar;
//
// import java.awt.*;
// import java.util.ArrayList;
// import java.util.List;
//
/// **
// * scroll region for javafx
// *
// * @author oortCloudFei
// */
// public class ScrollRegion {
//
//    public static final int SCROLL_BAR_WIDTH = 20;
//
//    /**
//     * scroll bars
//     */
//    private ScrollBar scrollBarBottom;
//    private ScrollBar scrollBarRight;
//
//    /**
//     * node root
//     */
//    private Group root;
//    private Group cur = new Group();
//
//    /**
//     * whxy
//     */
//    private double width;
//    private double height;
//    private double x;
//    private double y;
//
//    /**
//     * internal nodes
//     */
//    private List<Node> nodes = new ArrayList<>(16);
//
//    private double minX, minY, maxX, maxY;
//
//    public ScrollRegion(Group root, double width, double height, double x, double y) {
//
//        this.root = root;
//        this.width = width;
//        this.height = height;
//        this.x = x;
//        this.y = y;
//        // scroll bar add
//        scrollBarBottom = new ScrollBar();
//        scrollBarBottom.setPrefWidth(width);
//        scrollBarBottom.setPrefHeight(SCROLL_BAR_WIDTH);
//        scrollBarBottom.setTranslateX(x);
//        scrollBarBottom.setTranslateY(y + height - SCROLL_BAR_WIDTH);
//        scrollBarRight = new ScrollBar();
//        scrollBarRight.setOrientation(Orientation.VERTICAL);
//        scrollBarRight.setPrefHeight(height);
//        scrollBarRight.setPrefWidth(SCROLL_BAR_WIDTH);
//        scrollBarRight.setTranslateX(x + width - SCROLL_BAR_WIDTH);
//        scrollBarRight.setTranslateY(y);
//        minX = x;
//        maxX = x + width;
//        minY = y;
//        maxY = y + height;
//        // scroll bar listener
//        scrollBarBottom.valueProperty().addListener((ObservableValue<? extends Number> observable,
// Number oldValue, Number newValue) -> {
//            cur.setTranslateX(cur.getTranslateX() - (newValue.doubleValue() -
// oldValue.doubleValue()));
////            nodes.forEach(node -> {
////                visibleControl(node);
////            });
//        });
//        scrollBarRight.valueProperty().addListener((ObservableValue<? extends Number> observable,
// Number oldValue, Number newValue) -> {
//            cur.setTranslateY(cur.getTranslateY() - (newValue.doubleValue() -
// oldValue.doubleValue()));
//            nodes.forEach(node -> {
//                visibleControl(node);
//            });
//        });
//        ObservableList<Node> children = root.getChildren();
//        children.add(scrollBarBottom);
//        children.add(scrollBarRight);
//
//        root.getChildren().add(cur);
//    }
//
//    /**
//     * node add
//     * @param node
//     */
//    public void add(Node node) {
//
//        nodes.add(node);
//        cur.getChildren().add(node);
////        visibleControl(node);
//        scrollMMReset(node);
//    }
//
//    /**
//     * if node out, set visible to false
//     * @param node
//     */
//    private void visibleControl(Node node) {
//
//        Bounds layoutBounds = node.getLayoutBounds();
//        double curX = layoutBounds.getMinX() + cur.getTranslateX();
//        double curY = layoutBounds.getMinY() + cur.getTranslateY();
//        // 判断是否出区
//        if(curX > x + width || curX < x) {
//            node.setVisible(false);
//            return;
//        } else {
//            node.setVisible(true);
//        }
//        if(curY > y + height || curY < y) {
//            node.setVisible(false);
//            return;
//        } else {
//            node.setVisible(true);
//        }
//    }
//
//    /**
//     * scroll bar size reset
//     * @param node
//     */
//    private void scrollMMReset(Node node) {
//        Bounds layoutBounds = node.getLayoutBounds();
//        minX = Math.min(layoutBounds.getMinX(), minX);
//        maxX = Math.max(layoutBounds.getMaxX(), maxX);
//        minY = Math.min(layoutBounds.getMinY(), minY);
//        maxY = Math.max(layoutBounds.getMaxY(), maxY);
////        double bottomHalf = Math.abs(maxX - minX) / 2;
////        double rightHalf = Math.abs(maxY - minY) / 2;
////        scrollBarBottom.setMin(-bottomHalf);
////        scrollBarBottom.setMax(bottomHalf);
////        scrollBarRight.setMin(-rightHalf);
////        scrollBarRight.setMax(rightHalf);
//        scrollBarBottom.setMin(0);
//        scrollBarBottom.setMax(maxX);
//        scrollBarRight.setMin(0);
//        scrollBarRight.setMax(maxY);
//    }
//
//    public double getX() {
//        return x;
//    }
//
//    public double getY() {
//        return y;
//    }
// }
