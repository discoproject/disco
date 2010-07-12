
$(document).ready(function () {
    var job = new Job(document.location.search.substr(6));
    $("#hd #title").append(job.name);
    $("#kill_job").click(job.kill);
    $("#clean_job").click(job.clean);
    $("#purge_job").click(job.purge);
    $("#find_page").click(job.find);
});

function repeat(func, every, until) {
  if (until && until.call())
    return;
  func.call();
  setTimeout(function () { repeat(func, every, until); }, every);
}

function show_status() {
  document.location.href = "/index.html";
}

function Job(name) {
  self = this; /* cant actually use 'this' in methods since jquery binds it */
  self.name = name;

  self.kill = function () {
    if (confirm("Do you really want to kill " + self.name + "?"))
      post_req("/disco/ctrl/kill_job", JSON.stringify(self.name));
  }

  self.clean = function () {
    if (confirm("Do you really want to clean the job records of " + self.name + "?"))
      post_req("/disco/ctrl/clean_job", JSON.stringify(self.name), show_status);
  }

  self.purge = function () {
    if (confirm("Do you really want to delete all data of " + self.name + "?"))
      post_req("/disco/ctrl/purge_job", JSON.stringify(self.name), show_status);
  }

  self.find = function () {
    $.getJSON("/disco/ctrl/jobevents", {name: self.name,
                                        num: 200,
                                        filter: $("#pagesearch").val()},
      self.update_events);
  }

  self.isactive = function () {
    return self.status == "active";
  }

  self.request_info = function () {
    $.getJSON("/disco/ctrl/jobinfo", {name: self.name}, self.update_info);
  }

  self.update_info = function (data) {
    self.status = data.active;
    self.started = data.timestamp;

    if (self.isactive()) {
      $("#clean_job").hide();
      $("#kill_job").show();
    } else {
      $("#clean_job").show();
      $("#kill_job").hide();
    }

    $("#nfo_status").text(self.status);
    $("#nfo_started").text(self.started);
    $("#nfo_map").html(make_jobinfo_row(data.mapi, "Map"));
    $("#nfo_red").html(make_jobinfo_row(data.redi, "Reduce"));

    if (data.inputs.length >= 100) {
      $("#map_inputs").html("Showing the first 100 inputs<br/>" +
                            prepare_urls(data.inputs.slice(0, 100)));
    } else {
      $("#map_inputs").html(prepare_urls(data.inputs));
    }
    $("#cur_nodes").html(data.nodes.join("<br/>"));
    $("#results").html(prepare_urls(data.results));

    $(".url:odd").css({"background": "#eee"});
  }

  self.request_events = function () {
    $.getJSON("/disco/ctrl/jobevents", {name: self.name, num: 100},
              self.update_events);
  }

  self.update_events = function (events) {
    if ($("#pagesearch").val())
      return;

    $("#jobevents").html($.map(events, make_event));
    $(".jobevent .node").click(click_node);
  }

  repeat(self.request_info, 10000);
  repeat(self.request_events, 10000, self.isactive);
}

function make_jobinfo_row(dlist, mode) {
  return $.map([mode].concat(dlist), function(X, i) {
      if (X == mode)
        return $.create("td", {"class":"ttitle"}, [X]);
      else
        return $.create("td", {}, [String(X)]);
    });
}

function prepare_urls(lst) {
  return $.map(lst, function (X, i) {
      var t = "";
      if (typeof(X) == "string")
        t = X;
      else {
        X.reverse();
        t = X.shift();
        if (X.length)
          for (i in X)
            t += "<div class='redun'>(" + X[i] + ")</div>";
      }
      return "<div class='url'>" + t + "</div>";
    }).join("");
}

function click_node() {
  if ($(this).attr("locked")) {
    $(".jobevent").show();
    $(".jobevent .node:contains(" + $(this).text() + ")").
      removeAttr("locked").
      removeClass("locked");
  } else {
    $(".jobevent").hide();
    $(".jobevent .node:contains(" + $(this).text() + ")").
      attr({"locked": true}).
      addClass("locked").
      parent(".jobevent").show();
  }
}

function make_event(E, i) {
  var tstamp = E[0];
  var host = E[1];
  var msg = E[2];

  if (msg.match("^WARN"))
    id = "ev_warning";
  else if (msg.match("^ERROR"))
    id = "ev_error";
  else if (msg.match("^READY"))
    id = "ev_ready";
  else
    id = "";

  var msg = $.map(msg.split("\n"), function(X, i) {
      return $.create("pre", {}, [X])
    });

  var body = [$.create("span", {"class": "tstamp"}, [tstamp]),
              $.create("span", {"class": "node"}, [host]),
              $.create("div", {"class": "evtext", "id": id}, msg)];
  return $.create("div", {"class": "jobevent"}, body);
}
