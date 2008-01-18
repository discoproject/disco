
$(document).ready(function(){
        $("#hd #title").append(document.location.search.substr(6));
        $.getJSON("/disco/ctrl/jobinfo" + document.location.search,
                update_jobinfo);
        $.getJSON("/disco/ctrl/jobevents" + document.location.search +
                "&last=-1", update_events);
});

function update_jobinfo(data){
        if (data.active)
                $("#nfo_active").text("active")
        else
                $("#nfo_active").text("dead")
        $("#nfo_started").text(data.timestamp);
        $("#nfo_maps").text(data.nmap);
        $("#nfo_part").text(data.nred);
        $("#nfo_reduce").text(data.reduce);

        $("#map_inputs").html(data.inputs.join("<br/>"));
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

function update_events(events){
        if (events.length > 1){
                $(".jobevents").html($.map(events.reverse(), make_event));
                $(".jobevent .node").click(click_node);
        }else
                $(".jobevents").prepend(make_event(events[0]));

        if ($("#nfo_active").text() == "active")
                setTimeout(function(){
                        $.getJSON("/disco/ctrl/jobevents" +
                                document.location.search + 
                                        "&last=-1", update_events);
                }, 10000);
}

function make_event(E, i){
        if (E.msg.match("^WARN"))
                id = "ev_warning";
        else if (E.msg.match("^ERROR"))
                id = "ev_error";
        else
                id = ""
        var body = [$.create("span", {"class": "tstamp"}, [E.tstamp]),
                    $.create("span", {"class": "node"}, [E.host]),
                    $.create("div", {"class": "evtext", "id": id}, [E.msg])];
        return $.create("div", {"class": "jobevent"}, body);
}

