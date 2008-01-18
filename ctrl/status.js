
$(document).ready(function(){
        $.getJSON("/disco/ctrl/nodeinfo", update_nodeboxes);
});

function update_nodeboxes(Data)
{
        $(".yui-g").html($.map(Data.available, make_nodebox));
        $.each(Data.active, active_worker);
        
        setTimeout(function(){
                $.getJSON("/disco/ctrl/nodeinfo", update_nodeboxes);
        }, 10000);
}

function active_worker(i, W)
{
        var n = "_job_" + W.jobname.replace("@", "_");
        $(".status#" + W.node + " > .jbox#free:first")
                .addClass("busy").addClass(n).attr("id", "").click(
                        function(){
                                $(".joblist input").val(W.jobname);
                        });
}

function make_nodebox(B, i)
{
        var jboxes = $.map(Array(B.max_workers), function(X, i){
                return $.create("div", {"class": "jbox", "id": "free"}, []);
        });
        
        var sta = $.create("div", {"class": "status", "id": B.node}, jboxes);
        var tit = $.create("div", {"class": "title"}, [B.node]);
        var val_ok = $.create("div", {"class": "val lval"},
                [String(B.job_ok)]);
        var val_data = $.create("div", {"class": "val mval"},
                [String(B.data_error)]);
        var val_err = $.create("div", {"class": "val rval"},
                [String(B.error)]);

        return $.create("div", {"class": "nodebox"}, 
                [tit, sta, val_ok, val_data, val_err]);
}
