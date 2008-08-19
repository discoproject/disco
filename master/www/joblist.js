
/* http://www.wellstyled.com/tools/colorscheme2/index-en.html?tetrad;100;0;11;-1;-1;1;-0.7;0.25;1;0.5;1;-1;-1;1;-0.7;0.25;1;0.5;1;-1;-1;1;-0.7;0.25;1;0.5;1;-1;-1;1;-0.7;0.25;1;0.5;1;0 */

function job_mouseover()
{
        $(this).css('background', '#FFF7BF');
        $("._job_" + $(this).text().replace("@", "_")).
                addClass("show_jbox");
}

function job_mouseout()
{
        $(this).css('background', '#FFF');
        $(".show_jbox").removeClass("show_jbox");
}

function job_click()
{
        document.location.href = "job.html?name=" + $(this).text();
}

function filter_jobs(txt)
{
        $(".job").css("display", "none");
        $(".job:contains(" + txt + ")").css("display", "block");
}

function start_joblist()
{
        var options = {
                callback:filter_jobs,
                wait:500,
                highlight:true,
                enterkey:true
        }
        $(".joblist input").typeWatch(options);
        $.getJSON("/disco/ctrl/joblist", update_joblist);
}

function update_joblist(jobs)
{
        $(".jobs").html($.map(jobs, job_element));
        filter_jobs($(".joblist input").val());
        setTimeout(function(){
                $.getJSON("/disco/ctrl/joblist", update_joblist);
        }, 10000);
}

function job_element(job, i){
        var age = job[0];
        var job_status = job[1];
        var name = job[2];
        
        cbox = $.create("div", {"class": "live " + job_status}, []);
        jbox = $.create("div", {"class": "job"}, [cbox, name]);
        jbox.onmouseover = job_mouseover;
        jbox.onmouseout = job_mouseout;
        jbox.onclick = job_click;

        return jbox;
}


