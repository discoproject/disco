$(document).ready(function(){
    var job = new Job(document.location.search.substr(6));
    $("#hd #title").append(job.name);
    $("#kill_job").click(job.kill);
    $("#purge_job").click(job.purge);
    $("#find_page").click(job.find);
    $("#find_events").submit(job.find);
});

function repeat(func, every, until){
    if (until && until.call())
        return;
    func.call();
    setTimeout(function(){ repeat(func, every, until); }, every);
}

function show_status(){
    document.location.href = "/index.html";
}

function Job(name){
    self = this; /* cant actually use 'this' in methods since jquery binds it */
    self.name = name;

    self.kill = function(){
        if (confirm("Do you really want to kill " + self.name + "?"))
            post_req("/disco/ctrl/kill_job", JSON.stringify(self.name));
    }

    self.purge = function(){
        if (confirm("Do you really want to delete all data of " + self.name + "?"))
            post_req("/disco/ctrl/purge_job", JSON.stringify(self.name), show_status);
    }

    self.find = function(){
        self.request_events();
        return false;
    }

    self.isactive = function(){
        return self.status == "active";
    }

    self.request_info = function(){
        $.getJSON("/disco/ctrl/jobinfo", {name: self.name}, self.update_info);
    }

    self.update_info = function(data){
        self.status = data.active;
        self.started = data.timestamp;
        self.owner = data.owner;

        $("#nfo_status").text(self.status);
        $("#nfo_started").text(self.started);
        $("#nfo_owner").text(self.owner);
        $("#nfo_map").html(make_jobinfo_row(data.mapi, "Map"));
        $("#nfo_red").html(make_jobinfo_row(data.redi, "Reduce"));

        if (data.inputs.length >= 100) {
            $("#map_inputs").html("Showing the first 100 inputs<br/>" +
                                  prepare_urls(data.inputs.slice(0, 100)));
        } else {
            $("#map_inputs").html(prepare_urls(data.inputs));
        }
        $("#cur_nodes").html(data.hosts.join("<br/>"));
        $("#results").html(prepare_urls(data.results));

        $(".url:odd").css({"background": "#eee"});
    }

    self.request_events = function(){
        $.getJSON("/disco/ctrl/jobevents", {name: self.name,
                                            num: 100,
                                            filter: $("#pagesearch").val()},
                  self.update_events);
    }

    self.update_events = function(events){
        $(".events").html($.map(events, make_event));
        $(".event .node").click(click_node);
    }

    repeat(self.request_info, 10000);
    repeat(self.request_events, 10000, self.isactive);
}

function make_jobinfo_row(dlist, mode){
    return $.map([mode].concat(dlist), function(X, i){
        if (X == mode)
            return $.create("td", {"class":"title"}, [X]);
        else
            return $.create("td", {}, [String(X)]);
    });
}

function prepare_urls(lst){
    return $.map(lst, function(X, i){
        var t = "";
        if (typeof(X) == "string")
            t = X;
        else {
            X.reverse();
            t = X.shift();
            if (X.length)
                for (i in X)
                    t += "<div class='redundant'>(" + X[i] + ")</div>";
        }
        return "<div class='url'>" + t + "</div>";
    }).join("");
}

function click_node(){
    if ($(this).attr("locked")) {
        $(".event").show();
        $(".event .node:contains(" + $(this).text() + ")")
            .removeAttr("locked")
            .removeClass("locked");
    } else {
        $(".event").hide();
        $(".event .node:contains(" + $(this).text() + ")")
            .attr({"locked": true})
            .addClass("locked")
            .parent(".event").show();
    }
}

function make_event(E, i){
    var tstamp = E[0];
    var host = E[1];
    var msg = E[2];
    var type = (msg.match("^(WARNING|ERROR|READY)") || [""])[0].toLowerCase();

    var msg = $.map(msg.split("\n"), function(x, i){
        return $.create("pre", {}, [x])
    });

    var body = [$.create("div", {"class": "tstamp"}, [tstamp]),
                $.create("div", {"class": "node"}, [host]),
                $.create("div", {"class": "text " + type}, msg)];
    return $.create("div", {"class": "event"}, body);
}
