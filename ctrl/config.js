
$(document).ready(function(){
        $("#add_row").click(function(){
                $("tbody").append(
                        "<tr><td>&nbsp;</td><td>&nbsp;</td></tr>");
                $.uiTableEdit($("table"), {dataVerify: check_cell});
        });
        $("#save_table").click(send_table);
        $("#add_to_bl").click(add_to_blacklist);
        $("#save_settings").click(save_settings);
        $.getJSON("/disco/ctrl/load_config_table", new_table);
        $.getJSON("/disco/ctrl/get_blacklist", update_blacklist);
        $.getJSON("/disco/ctrl/get_settings", update_settings);
});

function show_msg(m){
        $("#msg").text(m.replace(/\"/g, "")).show().fadeOut(2000);
}

function post_req(url, data, func){
        $.ajax({data: data,
                dataType: "JSON", 
                url: url,
                type: "POST",
                processData: false,
                error: function(){ show_msg("Request failed"); },
                success: function(x){ 
                        show_msg(x);
                        if (func) func(x);
                }
        });
}

function update_blacklist(data){
        $("#blacklist").html($.map(data, function(item, i){
                return $.create("div", {"class": "bnode"}, [item]);
        }));
        $(".bnode").click(whitelist);
}

function whitelist(){
        post_req("/disco/ctrl/whitelist",
                JSON.stringify($(this).text()),
                function(){
                        $.getJSON("/disco/ctrl/get_blacklist",
                                update_blacklist);
        });
}

function save_settings(){
        var s = {}
        $.each($(".setting"), function(){
                s[$(this).attr("id")] = $(this).val();
        });
        post_req("/disco/ctrl/save_settings",
                JSON.stringify(s),
                function(){
                        $.getJSON("/disco/ctrl/get_settings",
                                update_settings);
        });
}

function update_settings(data){
        for (var k in data){
                $("#" + k).val(data[k]);                
        }
}

function add_to_blacklist(){
        post_req("/disco/ctrl/blacklist", 
                JSON.stringify($("#jobname").val()),
                function(){
                        $.getJSON("/disco/ctrl/get_blacklist",
                                update_blacklist);
        });
}

function send_table(){
        var table = $("tbody > tr").map(function(){
                return JSON.stringify($.makeArray($(this).children().map(
                        function(){return $(this).text();})));
        });
        jsonTable = ("[" + $.makeArray(table).join(",") + "]");
        post_req("/disco/ctrl/save_config_table", jsonTable);
}

function check_cell(val, orig, ev){
        $("tr > td:empty").parent().remove();
}

function new_table(data){
        $("tbody").html($.map(data, function(item, i){
                return $.create("tr", {}, $.map(item, function(item2, i){
                        return $.create("td", {}, [item2]);
                }));
        }));
        $.uiTableEdit($("table"), {dataVerify: check_cell});
}

