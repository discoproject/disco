
$(document).ready(function(){
    $("#hd #title").append(document.location.search.substr(6));
    $("#kill_job").click(kill_job);
    $("#clean_job").click(clean_job);
    $("#purge_job").click(purge_job);

    $("#find_page").click(function(){
        $.getJSON("/disco/ctrl/jobevents" +
            document.location.search +
            "&num=200&filter=" + $("#pagesearch").val(),
                update_events);
    });

    $.getJSON("/disco/ctrl/jobinfo" + document.location.search,
        update_jobinfo);
    $.getJSON("/disco/ctrl/jobevents" + document.location.search +
        "&num=100", update_events); 
});

function show_status(){
    document.location.href='/index.html';
}

function kill_job(){
    Name = document.location.search.substr(6);
    if (confirm("Do you really want to kill " + Name + "?"))
        post_req("/disco/ctrl/kill_job", JSON.stringify(Name));
}

function clean_job(){
    Name = document.location.search.substr(6);
    if (confirm("Do you really want to clean the job records of " + Name + "?"))
        post_req("/disco/ctrl/clean_job", JSON.stringify(Name), show_status);
}

function purge_job(){
    Name = document.location.search.substr(6);
    if (confirm("Do you really want to delete all data of " + Name + "?"))
        post_req("/disco/ctrl/purge_job", JSON.stringify(Name), show_status);
}

function make_jobinfo_row(dlist, mode){
    return $.map([mode].concat(dlist), function(X, i){
        if (X == mode)
            return $.create("td", {"class":"ttitle"}, [X]);
        else
            return $.create("td", {}, [String(X)]);
    });
}

function prepare_urls(lst){
    return $.map(lst, function(X, i){
        var t = "";
        if (typeof(X) == "string")
            t = X
        else{
            X.reverse();
            t = X.shift();
            if (X.length)
                for (i in X)
                    t += "<div class='redun'>(" 
                        + X[i] + ")</div>";
        }
        return "<div class='url'>" + t + "</div>";
    }).join("");
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
    $("#jobinfo_map").html(make_jobinfo_row(data.mapi, "Map"));
    $("#jobinfo_red").html(make_jobinfo_row(data.redi, "Reduce"));

    if (data.inputs.length >= 100){
        $("#map_inputs").html("Showing the first 100 inputs<br/>" + 
            prepare_urls(data.inputs.slice(0, 100)));
    }else{
        $("#map_inputs").html(prepare_urls(data.inputs));
    }
    $("#cur_nodes").html(data.nodes.join("<br/>"));
    $("#results").html(prepare_urls(data.results));
       
    $(".url:odd").css({"background": "#eee"});

    setTimeout(function(){
        $.getJSON("/disco/ctrl/jobinfo" + document.location.search,
            update_jobinfo);
    }, 10000);
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
    
    $(".jobevents").html($.map(events_msg, make_event)); 
    $(".jobevent .node").click(click_node);

    if ($("#nfo_active").text() == "active")
        setTimeout(function(){
            $.getJSON("/disco/ctrl/jobevents" +
                document.location.search + 
                    "&num=100", function(e, x){
                        update_events(e, x, "auto");
                });
        }, 10000);
}

function make_event(E, i){
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
        id = ""

    var msg = $.map(msg.split("\n"), 
        function(X, i){return $.create("pre", {}, [X])});

    var body = [$.create("span", {"class": "tstamp"}, [tstamp]),
            $.create("span", {"class": "node"}, [host]),
            $.create("div", {"class": "evtext", "id": id}, msg)];
    return $.create("div", {"class": "jobevent"}, body);
}

