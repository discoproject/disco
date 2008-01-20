
jQuery.fn.log = function (msg) {
      console.log("%s: %o", msg, this);
      return this;
 };

$(document).ready(function(){
        $.ajaxSetup({
                cache: false, 
        })

        start_joblist();
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
