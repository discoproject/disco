
/* http://www.wellstyled.com/tools/colorscheme2/index-en.html?tetrad;100;0;11;-1;-1;1;-0.7;0.25;1;0.5;1;-1;-1;1;-0.7;0.25;1;0.5;1;-1;-1;1;-0.7;0.25;1;0.5;1;-1;-1;1;-0.7;0.25;1;0.5;1;0 */

/* http://www.mjijackson.com/2008/02/rgb-to-hsl-and-rgb-to-hsv-color-model-conversion-algorithms-in-javascript */
function hslToRgb(h, s, l){
    var r, g, b;

    if(s == 0)
        r = g = b = l; // achromatic
    else {
        function hue2rgb(p, q, t){
            if(t < 0) t += 1;
            if(t > 1) t -= 1;
            if(t < 1/6) return p + (q - p) * 6 * t;
            if(t < 1/2) return q;
            if(t < 2/3) return p + (q - p) * (2/3 - t) * 6;
            return p;
        }

        var q = l < 0.5 ? l * (1 + s) : l + s - l * s;
        var p = 2 * l - q;
        r = hue2rgb(p, q, h + 1/3);
        g = hue2rgb(p, q, h);
        b = hue2rgb(p, q, h - 1/3);
    }

    return [r * 255, g * 255, b * 255];
}

function job_mouseover(){
    $(this).css('background', '#FFF7BF');
    $("._job_" + $(this).text().replace("@", "_").split(":").join(""))
        .addClass("show_jbox");
}

function job_mouseout(){
    $(this).css('background', '#FFF');
    $(".show_jbox").removeClass("show_jbox");
}

function job_click(){
    document.location.href = "job.html?name=" + $(this).text();
}

function filter_jobs(txt){
    $(".job").css("display", "none");
    $(".job:contains('" + txt + "')").css("display", "block");
}

function start_joblist(){
    var options = {callback: filter_jobs,
                   wait: 500,
                   highlight: true,
                   enterkey: true}
    $("#joblist input").typeWatch(options);
    $.getJSON("/disco/ctrl/joblist", update_joblist);
}

function update_joblist(jobs){
    $("#jobs").html($.map(jobs, job_element));
    filter_jobs($("#joblist input").val());
    setTimeout(function(){
        $.getJSON("/disco/ctrl/joblist", update_joblist);
    }, 10000);
}

function job_element(job, i){
    var prio = job[0];        /* [-1.0..1.0] */
    var job_status = job[1];
    var name = job[2];

    var sat = (prio + 1.0) / 2.0;
    var rgb = hslToRgb(0.145, 1.0 - sat, 0.5);

    cbox = $.create("div", {"class": "live " + job_status}, []);
    jbox = $.create("div", {"class": "job"}, [cbox, name]);
    jbox.onmouseover = job_mouseover;
    jbox.onmouseout = job_mouseout;
    jbox.onclick = job_click;

    return jbox;
}

$(document).ready(start_joblist);
