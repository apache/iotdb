package com.browniebytes.javafx.control;

import java.io.IOException;
import java.net.URL;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ResourceBundle;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.control.Accordion;
import javafx.scene.control.Button;
import javafx.scene.control.DatePicker;
import javafx.scene.control.TitledPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;

/**
 * This class includes code from taipeiben's DataTimePicker project.
 *
 * <p>Copyright: 2014-2015 taipeiben and/or other contributors
 *
 * <p>Project page: https://github.com/burmanm/gorilla-tsc
 *
 * <p>License: http://www.apache.org/licenses/LICENSE-2.0
 */
public class DateTimePickerPopup extends VBox implements Initializable {

  private final DateTimePicker parentControl;
  private final HoursPicker hoursPicker;
  private final MinuteSecondPicker minutesPicker;
  private final MinuteSecondPicker secondsPicker;

  private int hour;
  private int minute;
  private int second;

  @FXML private Accordion accordion;

  @FXML private DatePicker datePicker;

  @FXML private TitledPane timePane;

  @FXML private HBox timeButtonsPane;

  @FXML private Button hoursButton;

  @FXML private Button minutesButton;

  @FXML private Button secondsButton;

  public DateTimePickerPopup(final DateTimePicker parentControl) {
    this.hour = parentControl.dateTimeProperty().get().getHour();
    this.minute = parentControl.dateTimeProperty().get().getMinute();
    this.second = parentControl.dateTimeProperty().get().getSecond();

    this.parentControl = parentControl;
    this.hoursPicker = new HoursPicker(this);
    this.minutesPicker = new MinuteSecondPicker(this);
    this.secondsPicker = new MinuteSecondPicker(this);

    // Load FXML
    final FXMLLoader fxmlLoader =
        new FXMLLoader(getClass().getResource("DateTimePickerPopup.fxml"));
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
    // Set the button text
    setTimeButtonText();

    // Initialize date to value in parent control
    datePicker.valueProperty().set(parentControl.dateTimeProperty().get().toLocalDate());

    // Start with time pane expanded
    accordion.setExpandedPane(accordion.getPanes().get(1));
  }

  void setDate(final LocalDate date) {
    datePicker.setValue(date);
  }

  LocalDate getDate() {
    return datePicker.getValue();
  }

  void setTime(final LocalTime time) {
    hour = time.getHour();
    minute = time.getMinute();
    second = time.getSecond();
    setTimeButtonText();
  }

  LocalTime getTime() {
    return LocalTime.of(hour, minute, second);
  }

  int getHour() {
    return hour;
  }

  void restoreTimePanel() {
    // Update hour
    hour = hoursPicker.getHour();
    minute = minutesPicker.getValue();
    second = secondsPicker.getValue();
    setTimeButtonText();

    // Restore original panel
    timePane.setContent(timeButtonsPane);
  }

  @FXML
  void handleHoursButtonAction() {
    timePane.setContent(hoursPicker);
  }

  @FXML
  void handleMinutesButtonAction() {
    timePane.setContent(minutesPicker);
  }

  @FXML
  void handleSecondsButtonAction() {
    timePane.setContent(secondsPicker);
  }

  @FXML
  void handleOkButtonAction() {
    hour = hoursPicker.getHour();
    setTimeButtonText();

    parentControl.hidePopup();
  }

  private void setTimeButtonText() {
    hoursButton.setText(String.format("%02d", hour));
    minutesButton.setText(String.format("%02d", minute));
    secondsButton.setText(String.format("%02d", second));
  }
}
