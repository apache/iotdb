package com.browniebytes.javafx.control;

import java.io.IOException;
import java.net.URL;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.ResourceBundle;
import javafx.beans.property.ObjectProperty;
import javafx.beans.property.SimpleObjectProperty;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.control.Button;
import javafx.scene.control.TextField;
import javafx.scene.layout.HBox;
import javafx.stage.Popup;
import javafx.stage.Window;

/**
 * This class includes code from taipeiben's DataTimePicker project.
 *
 * <p>Copyright: 2014-2015 taipeiben and/or other contributors
 *
 * <p>Project page: https://github.com/burmanm/gorilla-tsc
 *
 * <p>License: http://www.apache.org/licenses/LICENSE-2.0
 */
public class DateTimePicker extends HBox implements Initializable {

  // DateTime value
  private ObjectProperty<LocalDateTime> dateTime;

  // Formatter for the text field
  private final DateTimeFormatter formatter;

  // Will hold date and time selectors
  private final Popup popupContainer;

  // Contents of the popup
  private final DateTimePickerPopup popup;

  @FXML private TextField textField;

  @FXML private Button button;

  /** Default constructor uses current date and time with default formatter. */
  public DateTimePicker() {
    this(LocalDateTime.now());
  }

  /**
   * Creates a DateTimePicker with an initial LocalDateTime. A default formatter is used.
   *
   * @param dateTime LocalDateTime to use initially
   */
  public DateTimePicker(final LocalDateTime dateTime) {
    this(dateTime, DateTimeFormatter.ofLocalizedDateTime(FormatStyle.SHORT));
  }

  /**
   * Creates a new DateTimePicker with the date and time set to the supplied LocalDateTime and
   * supplied formatter.
   *
   * @param dateTime Sets date and time to this LocalDateTime
   * @param formatter Formatter to use to display date and time value
   */
  public DateTimePicker(final LocalDateTime dateTime, final DateTimeFormatter formatter) {
    // Set instance variables
    this.dateTime = new SimpleObjectProperty<LocalDateTime>(dateTime);
    this.formatter = formatter;
    this.popupContainer = new Popup();
    this.popup = new DateTimePickerPopup(this);

    // Load FXML
    final FXMLLoader fxmlLoader = new FXMLLoader(getClass().getResource("DateTimePicker.fxml"));
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
    textField.setText(formatter.format(dateTime.get()));

    dateTime.addListener(
        (observable, oldValue, newValue) -> {
          popup.setDate(newValue.toLocalDate());
          popup.setTime(newValue.toLocalTime());

          textField.setText(formatter.format(newValue));
        });

    button.prefHeightProperty().bind(textField.heightProperty());

    popupContainer.getContent().add(popup);
    popupContainer.autoHideProperty().set(true);
  }

  /**
   * Gets the current LocalDateTime value
   *
   * @return The current LocalDateTime value
   */
  public ObjectProperty<LocalDateTime> dateTimeProperty() {
    return dateTime;
  }

  void hidePopup() {
    // Closing popup, so need to get date and time from it.
    final LocalDate date = popup.getDate();
    final LocalTime time = popup.getTime();
    dateTime.setValue(LocalDateTime.of(date, time));
    textField.setText(formatter.format(dateTime.get()));
    popupContainer.hide();
  }

  @FXML
  void handleButtonAction(ActionEvent event) {
    // If popup is showing, hide it.  Otherwise, position and show it.
    if (popupContainer.isShowing()) {
      popupContainer.hide();
    } else {
      final Window window = button.getScene().getWindow();

      final double x =
          window.getX() + textField.localToScene(0, 0).getX() + textField.getScene().getX();
      final double y =
          window.getY()
              + button.localToScene(0, 0).getY()
              + button.getScene().getY()
              + button.getHeight();

      popupContainer.show(this.getParent(), x, y);
    }
  }
}
