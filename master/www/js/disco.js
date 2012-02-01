jQuery.fn.log = function (msg){
    console.log("%s: %o", msg, this);
    return this;
};

$(document).ready(function(){
    $.ajaxSetup({cache: false});
    $.getJSON("/disco/version", function (data){
        $("#version").text(data);
    });
});

function format_size(num){
    units = ['KB', 'MB', 'GB', 'TB'];
    for (i = 0; i < units.length; i++) {
	if (num < 1024)
	    return num.toPrecision(3) + units[i];
	num /= 1024;
    }
}

function show_msg(m){
    $("#msg").text(m.replace(/\"/g, "")).show().fadeOut(2000);
}

function post_req(url, data, func){
    $.ajax({data: data,
            dataType: "JSON",
            url: url,
            type: "POST",
            processData: false,
            error: function (XMLHttpRequest, textStatus, errorThrown){
		show_msg("Request to " + url + " failed: " + textStatus);
	    },
            success: function (x){
		show_msg(x);
		if (func)
		    func(x);
	    }
	   });
}
