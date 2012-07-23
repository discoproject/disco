$(document).ready(function(){
    $.getJSON("/disco/ctrl/nodeinfo", update_nodeboxes);
    $.getJSON("/ddfs/ctrl/gc_stats", update_gcstats);
    $.getJSON("/ddfs/ctrl/gc_status", update_gcstatus);
});

function Node(host, info){
    self = this; /* cant actually use 'this' in methods since jquery binds it */
    self.host = host;
    self.info = info;
    self.id = host.replace(/\./g, "-");

    self.append_to = function(elmt){
        var jboxes = $.map(Array(self.info.max_workers), function(X, i){
            return $.create("div", {"class": "jbox", "id": "free"}, []);
        });

        var disconnected = self.info.connected ? "" : " disconnected";
        var title = $.create("div", {"class": "title" + disconnected}, [host]);
        var status = $.create("div", {"class": "status", "id": self.id}, jboxes);
        var diskp = 100 * self.info.diskfree / (self.info.diskfree + self.info.diskused);
        var disk = $.create("div", {"class": "disk"});
        var diskused = $(disk).append($.create("div", {"class": "disk used",
                                                       "style": "width: " + diskp + "%"},
                                               [format_size(self.info.diskfree)]));
        var val_ok = $.create("div", {"class": "val lval"},
                              [String(self.info.job_ok)]);
        var val_data = $.create("div", {"class": "val mval"},
                                [String(self.info.data_error)]);
        var val_err = $.create("div", {"class": "val rval"},
                               [String(self.info.error)]);
        var blacklisted = self.info.blacklisted ? "blacklisted" : "";

        elmt.append($.create("div", {"class": "nodebox " + blacklisted},
                             [title, status, disk, val_ok, val_data, val_err]));
        $.map(self.info.tasks || [], self.show_task);
    }

    self.show_task = function(task){
        $(".status#" + self.id + " > .jbox#free:first")
            .attr("id", "")
            .addClass("busy")
            .addClass("_job_" + task.replace("@", "_").split(":").join(""))
            .click(function(){
                $("#joblist input").val(task);
            });
    }
}

function update_nodeboxes(data){
    $("#nodes").empty();
    var hosts = [];
    for (host in data)
        hosts.push(host);
    hosts.sort();
    $.each(hosts, function(i, host){
        new Node(host, data[host]).append_to($("#nodes"));
    });

    setTimeout(function(){
        $.getJSON("/disco/ctrl/nodeinfo", update_nodeboxes);
    }, 10000);
}

function update_gcstats(data){
    $("#gcstats").empty();
    if (typeof(data) === "string")
        $("#gcstats").text(data);
    else {
        $("#gcstats").append(String("At: " + data["timestamp"]));
        var thd = $("<thead><tr><td/> <td>Files</td> <td>Bytes</td></tr></thead>");
        var tbd = $.create("tbody", {}, []);
        $.each(data["stats"], function(typ, stats){
            $(tbd).append($.create("tr", {"class": "gcstat"},
                                   [$.create("td", {}, [String(typ)]),
                                    $.create("td", {}, [String(stats[0])]),
                                    $.create("td", {}, [String(format_size(stats[1]/1000))])]));
        });
        $("#gcstats").append($($("<table class='gcstats_table'/>")).append(thd, tbd));
    }

    setTimeout(function(){
        $.getJSON("/ddfs/ctrl/gc_stats", update_gcstats);
    }, 10000);
}

function update_gcstatus(data){
    $("#gcstatus").unbind('click').empty();
    if (data === "")
        $("#gcstatus").append('<a href="#">Start GC</a>').show().click(start_gc);
    else
        $("#gcstatus").text(data);
    setTimeout(function(){
        $.getJSON("/ddfs/ctrl/gc_status", update_gcstatus);
    }, 10000);
}

function start_gc(){
    $.getJSON("/ddfs/ctrl/gc_start", function(resp){
        $("#gcstatus").unbind('click').text(resp).show().fadeOut(2000);
    });
}
