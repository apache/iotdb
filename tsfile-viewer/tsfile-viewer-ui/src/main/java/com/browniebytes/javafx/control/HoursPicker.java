package com.browniebytes.javafx.control;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.control.Toggle;
import javafx.scene.control.ToggleButton;
import javafx.scene.control.ToggleGroup;
import javafx.scene.layout.GridPane;

/**
 * This class includes code from taipeiben's DataTimePicker project.
 *
 * <p>Copyright: 2014-2015 taipeiben and/or other contributors
 *
 * <p>Project page: https://github.com/burmanm/gorilla-tsc
 *
 * <p>License: http://www.apache.org/licenses/LICENSE-2.0
 */
public class HoursPicker extends GridPane implements Initializable {

  private static final int NUM_BUTTONS = 12;

  private final List<ToggleButton> buttonList = new ArrayList<ToggleButton>(NUM_BUTTONS);

  private final DateTimePickerPopup parentContainer;

  @FXML private ToggleGroup hoursToggleGroup;

  @FXML private ToggleButton zeroButton;

  @FXML private ToggleButton oneButton;

  @FXML private ToggleButton twoButton;

  @FXML private ToggleButton threeButton;

  @FXML private ToggleButton fourButton;

  @FXML private ToggleButton fiveButton;

  @FXML private ToggleButton sixButton;

  @FXML private ToggleButton sevenButton;

  @FXML private ToggleButton eightButton;

  @FXML private ToggleButton nineButton;

  @FXML private ToggleButton tenButton;

  @FXML private ToggleButton elevenButton;

  @FXML private ToggleButton amPmButton;

  HoursPicker(DateTimePickerPopup parentContainer) {

    this.parentContainer = parentContainer;

    // Load FXML
    final FXMLLoader fxmlLoader = new FXMLLoader(getClass().getResource("HoursPicker.fxml"));
    fxmlLoader.setRoot(this);
    fxmlLoader.setController(this);

    try {
      fxmlLoader.load();
    } catch (IOException ex) {
      // Should never happen.  If it does however, we cannot recover
      // from this
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void initialize(URL location, ResourceBundle resources) {
    buttonList.add(zeroButton);
    buttonList.add(oneButton);
    buttonList.add(twoButton);
    buttonList.add(threeButton);
    buttonList.add(fourButton);
    buttonList.add(fiveButton);
    buttonList.add(sixButton);
    buttonList.add(sevenButton);
    buttonList.add(eightButton);
    buttonList.add(nineButton);
    buttonList.add(tenButton);
    buttonList.add(elevenButton);

    // Size the AM/PM toggle button
    amPmButton
        .prefHeightProperty()
        .bind(zeroButton.heightProperty().multiply(3).add(getHgap() * 2));
    amPmButton.prefWidthProperty().set(35);

    // Configure AM/PM toggle button change listener
    amPmButton
        .selectedProperty()
        .addListener(
            new ChangeListener<Boolean>() {
              @Override
              public void changed(
                  final ObservableValue<? extends Boolean> observable,
                  final Boolean oldValue,
                  final Boolean newValue) {
                // Offset added for times after noon and before midnight
                int offset = 0;

                // Toggle "AM" and "PM" and add 12 to the offset for PM
                if (newValue) {
                  amPmButton.setText("PM");
                  offset = NUM_BUTTONS;
                } else {
                  amPmButton.setText("AM");
                }

                // Update button text
                for (int i = 0; i < NUM_BUTTONS; i++) {
                  buttonList.get(i).setText(String.format("%02d", i + offset));
                }

                // TODO: update hour value
              }
            });

    // Set hour to the value stored in the parent
    int hour = parentContainer.getHour();

    // Select button
    int offset = 0;
    if (hour > 11) {
      amPmButton.setSelected(true);
      offset = -12;
    }
    hoursToggleGroup.selectToggle(buttonList.get(hour + offset));

    // Change listener ensures that an hour value is always selected
    hoursToggleGroup
        .selectedToggleProperty()
        .addListener(
            new ChangeListener<Toggle>() {
              @Override
              public void changed(
                  ObservableValue<? extends Toggle> observable, Toggle oldValue, Toggle newValue) {

                // If user clicks the same value, set it back to the
                // old value and do nothing
                if (newValue == null) {
                  hoursToggleGroup.selectToggle(oldValue);
                } else {
                  parentContainer.restoreTimePanel();
                }
              }
            });
  }

  int getHour() {
    int hour = buttonList.indexOf(hoursToggleGroup.getSelectedToggle());
    if (amPmButton.isSelected()) {
      hour += 12;
    }
    return hour;
  }
}
