
$(document).ready(function(){
        $("#add_row").click(function(){
                $("tbody").append(
                        "<tr><td><a class='remove' href='#'>remove</a></td><td class='editable'>&nbsp;</td><td class='editable'>&nbsp;</td></tr>");
                $.uiTableEdit($("table"), {find: 'tbody > tr > td.editable', dataVerify: check_cell});
        });
        $("#save_table").click(send_table);
        $("#add_to_bl").click(add_to_blacklist);
        $("#save_settings").click(save_settings);
        $.getJSON("/disco/ctrl/load_config_table", new_table);
        $.getJSON("/disco/ctrl/get_blacklist", update_blacklist);
        $.getJSON("/disco/ctrl/get_settings", update_settings);
        $('tbody').click(function(event) {	// event delegation - to hook event handlers for dynamic contents
                var $real_target = $(event.target);
                if ($real_target.is('a.remove')) {
                    $real_target.parents("tr").remove();
                }
        });
});

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
                var arr = $.makeArray($(this).children().map(function(){
                        return jQuery.trim($(this).text() || $(this).find('input').val());}));
                arr.shift();  // throw away the first node - 'remove'
                return JSON.stringify(arr);
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
                        if (i % 2 == 0) 
                                return [$.create("td", {}, [$.create("a", {'class':'remove','href':'#'}, ["remove"])]), 
                                        $.create("td", {'class':'editable'}, [item2])];
			else
                                return $.create("td", {'class':'editable'}, [item2]);
                }));
        }));
        $.uiTableEdit($("table"), {find: 'tbody > tr > td.editable', dataVerify: check_cell});

}

