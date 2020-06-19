/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
 */
var historyValues = [];
var historyCursor = 0;

var session_id = null;

function postData(url, data) {
  // Default options are marked with *
  return fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': 'Basic cm9vdDpyb290',
    },
    body: JSON.stringify(data),
  })
  .then(response => response.text()) // parses response to JSON
}

function submitCommand(text, dontClearInput) {
  historyValues.push(text);
  historyCursor = historyValues.length;

  append(escapeHtml(text), "input", escapeHtml("> "),true);
  scrollDown();

  if (!dontClearInput) {
    $("#input").val("");
  }

  var url = "http://localhost:8181/rest/sql"

  const data = { "sql": text };

  fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': 'Basic cm9vdDpyb290',
    },
    body: JSON.stringify(data),
  })
  .then(response => response.json())
  .then(data => {
    console.log('Success:', data);
    // $("#log").append(data);
    // append("<a href=\"#run\">" + escapeHtml(data)+"</a>", "input", escapeHtml("> "),true);
    // console.log(data.length)
    // console.log(data[0].length);

    var html = '<table style="border-collapse: collapse;"><tr>';
    for (i = 0; i < data.length; i++){
      for (j = 0; j < data[0].length; j++){
          html += '<td>'+data[i][j]+'</td>';
      }
      html += '</tr>'
    }
    html+='</table>'
    // $("#log").append(html);
    $("#toolbar").before(html);
    $("#toolbar").show();
    $("#input").select();
    // append(html, "response");
    scrollDown();
  })
  .catch((error) => {
    // console.error('Error:', error);
    // append(response.text(), "error");
    postData(url,data)
    .then(data => {
      // $("#toolbar").before(data);
      append(data,"error");
      $("#toolbar").show();
      $("#input").select();
      scrollDown();
    }
    )

  });
    
};

function append(str, klass, prefix, isHtml) {
  if (prefix === undefined) {
    prefix = "";
  }

  if (!isHtml) {
    prefix = escapeHtml(prefix);
    str = escapeHtml(str);
  }

  var message =
    '<div class="line ' + klass + '">' +
    '<div class="nopad">' +
    '<span class="prompt">' + prefix + '</span>' +
    str +
    '</div></div>';

  // $("#log").append(message);
  $("#toolbar").hide();
  $("#toolbar").before(message);
};

function scrollDown() {
  $("#log").scrollTop($("#log").prop("scrollHeight"));
};

function escapeHtml(str) {
  str = str.replace(/&/g, "&amp;");
  str = str.replace(/</g, "&lt;");
  str = str.replace(/>/g, "&gt;");
  str = str.replace(/\n/g, "<br>");

  return str;
};

function cursorToEnd(input, text) {
  input.val(text);
  setCaretToPos(input.get(0), text.length);
};

function setSelectionRange(input, selectionStart, selectionEnd) {
  if (input.setSelectionRange) {
    input.focus();
    input.setSelectionRange(selectionStart, selectionEnd);
  } else if (input.createTextRange) {
    var range = input.createTextRange();
    range.collapse(true);
    range.moveEnd('character', selectionEnd);
    range.moveStart('character', selectionStart);
    range.select();
  }
};

function setCaretToPos(input, pos) {
  setSelectionRange(input, pos, pos);
};

$(document).ready(function () {
  $("#input").focus();

  $("#input").keydown(function (event) {
    if (event.keyCode == 13) {
      var text = $("#input").val().trim();
      console.log(text);
      if(text!="") submitCommand(text);

      return false;
    } else if (event.keyCode == 38) {
      if (historyCursor > 0) {
        var text = historyValues[--historyCursor];
        cursorToEnd($("#input"), text);
      }

      return false;
    } else if (event.keyCode == 40) {
      if (historyCursor < historyValues.length - 1) {
        var text = historyValues[++historyCursor];
        cursorToEnd($("#input"), text);
      } else {
        historyCursor = historyValues.length
        $("#input").val("");
      }

      return false;
    }
  });

  $("#toolbar").slideDown(500, function () {
    $("#input").focus();
  });
  $(document).on('click', "a[href='#help']", function () {
    submitCommand("help " + $(this).text());
    return false;
  });
  $(document).on('click', "a[href='#run']", function () {
    submitCommand($(this).text());
    return false;
  });
  $(document).on('click', "a[href='#comment']", function () {
    return false;
  });


  /*
  $("a[data-run-command]").live('click',function () {
    submitCommand($.data(this,'run-command'))
    return false;
  });
  $("a[href^='#']").live('click',function () {
    var cmd=unescape($(this).attr('href').substr(1));
    submitCommand(cmd);
    return false;
  });
  */
});
