
$(document).ready(function(){
        $("#hd #title").append(document.location.search.substr(6));
        $("#kill_job").click(kill_job);
        $("#clean_job").click(clean_job);

        $("#show_page").click(function(){
                r = $("#pagerange").val().split("-");
                if (r.length == 1)
                        r = [r[0], parseInt(r[0]) + 100];
                if (r[0] < r[1]){
                        $.getJSON("/disco/ctrl/jobevents" + 
                                document.location.search +
                                "&offs=" + r[0] + "&num=" + (r[1] - r[0]), 
                                        update_events); 
                }else{
                        $("#pagerange").val("");
                }
        });

        $("#find_page").click(function(){
                $.getJSON("/disco/ctrl/jobevents" +
                        document.location.search +
                        "&find=" + $("#pagesearch").val(),
                                update_events);
        });

        $.getJSON("/disco/ctrl/jobinfo" + document.location.search,
                update_jobinfo);
        $.getJSON("/disco/ctrl/jobevents" + document.location.search +
                "&offs=0&num=100", update_events); 
});

function kill_job(){
        Name = document.location.search.substr(6);
        if (confirm("Do you really want to kill " + Name + "?"))
                post_req("/disco/ctrl/kill_job", JSON.stringify(Name));
}

function clean_job(){
        Name = document.location.search.substr(6);
        if (confirm("Do you really want to clean the job records of " + Name + "?"))
                post_req("/disco/ctrl/clean_job", JSON.stringify(Name));
}

function update_jobinfo(data){
        $("#nfo_active").text(data.active);
        if (data.active == "active"){
                $("#clean_job").hide();
                $("#kill_job").show();
        }else{
                $("#kill_job").hide();
                $("#clean_job").show();
        }
        $("#nfo_started").text(data.timestamp);
        $("#nfo_maps").text(data.nmap);
        $("#nfo_part").text(data.nred);
        $("#nfo_reduce").text(data.reduce);

        $("#map_inputs").html(data.inputs.join("<br/>"));
        $("#cur_nodes").html(data.nodes.join("<br/>"));
        $("#results").html(data.results.join("<br/>"));
}

function click_node()
{
        if ($(this).attr("locked")){
                $(".jobevent").show();
                $(".jobevent .node:contains(" + 
                        $(this).text() + ")").removeAttr("locked").
                                removeClass("locked")
        }else{
                $(".jobevent").hide();
                $(".jobevent .node:contains(" + 
                        $(this).text() + ")").
                        attr({"locked": true}).
                        addClass("locked").
                        parent(".jobevent").show();
        }
}

function update_events(events_msg, success, is_auto){
        if ($("#pagesearch").val() && is_auto != undefined)
                return;
        if ($("#pagerange").val() && is_auto != undefined)
                return;
        
        if (events_msg[0] > 0){
                if (events_msg[0] < 100)
                        events_msg[0] = 100;
                $("#prev_page").show().unbind('click').click(function(){
                        $.getJSON("/disco/ctrl/jobevents" +
                                document.location.search + 
                                "&offs=" + (events_msg[0] - 100) +
                                "&num=100", update_events)
                }).text("newer events (" + (events_msg[0] - 100) + "-" + 
                        (events_msg[0]) + ")");
        }else
                $("#prev_page").hide();

        if (events_msg[2]){
                $("#next_page").show().unbind('click').click(function(){
                        $.getJSON("/disco/ctrl/jobevents" +
                                document.location.search + 
                                "&offs=" + (events_msg[0] + events_msg[1]) +
                                "&num=100", update_events);
                }).text("older events (" + (events_msg[0] + events_msg[1]) + "-" + 
                        (events_msg[0] + events_msg[1] + 100) + ")");
        }else
                $("#next_page").hide();

        $(".jobevents").html($.map(events_msg[3], make_event)); 
        $(".jobevent .node").click(click_node);

        if ($("#nfo_active").text() == "active" && events_msg[0] == 0)
                setTimeout(function(){
                        $.getJSON("/disco/ctrl/jobevents" +
                                document.location.search + 
                                        "&offs=0&num=100", function(e, x){
                                                update_events(e, x, "auto");
                                });
                }, 10000);
}

function make_event(E, i){
        if (E.msg.match("^WARN"))
                id = "ev_warning";
        else if (E.msg.match("^ERROR"))
                id = "ev_error";
        else if (E.msg.match("^READY"))
                id = "ev_ready";
        else
                id = ""

        var msg = $.map(E.msg.split("\n"), 
                function(X, i){return $.create("pre", {}, [X])});

        var body = [$.create("span", {"class": "tstamp"}, [E.tstamp]),
                    $.create("span", {"class": "node"}, [E.host]),
                    $.create("div", {"class": "evtext", "id": id}, msg)];
        return $.create("div", {"class": "jobevent"}, body);
}

