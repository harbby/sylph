
$(function () {
    url = "/_sys/job_manger"
    var send = {
        "type": "list", "jobId": ""
    };
    $.ajax({
        type: "post",
        url: url,
        contentType: "application/json;charset=UTF-8",
        dataType: "json",
        data: JSON.stringify(send),
        success : function(data){
            list = data.data;
            for (var i = 0; i < list.length; i++) {
                var jobId = list[i].jobId;
                var create_time = list[i].create_time
                var yarnId = list[i].yarnId
                var status = list[i].status;
                var type = list[i].type;
                var app_url = list[i].app_url;
                var button = '';
                switch (status) {
                    case 'RUNNING':
                        status = '上线';
                        button = '<button class="btn btn-primary stop">下线</button>';
                        break;
                    case 'STOP':
                        status = '下线';
                        button = '<button class="btn btn-primary active">上线</button>' + '<button class="btn btn-primary delete">删除</button>' + '<button class="btn btn-primary btn_edit" data-id="' + jobId + '" data-type="'+type+'">编辑</button>';
                        break;
                    case 'STARTING':
                        status = '正在启动中';
                        //button = '<button class="btn btn-primary active">上线</button>' + '<button class="btn btn-primary delete">删除</button>';
                        break;
                    case 'START_ERROR':
                        status = '启动失败';
                        button = '<button class="btn btn-primary stop">下线</button>';
                        break;
                    default:  //未知状态
                    //status = '未知状态:';
                }
                if (yarnId != null && yarnId != '') {
                    yarnId = '<a href="' + app_url + '" target="_blank">' + yarnId + '</a>';
                }
                var tmp =
                    '<div class="row">' +
                    '<div class="col-md-2">' + jobId + '</div>' +
                    '<div class="col-md-3">' + yarnId + '</div>' +
                    '<div class="col-md-1">' + type + '</div>' +
                    '<div class="col-md-2">' + create_time + '</div>' +
                    '<div class="col-md-1">' + status + '</div>' +
                    '<div class="col-md-3" jobId="' + jobId + '">' + button + '</div>' +
                    '</div>';
                $('#rowHead').after(tmp);
            }
        },
        error: function(XMLHttpRequest, textStatus, errorThrown) {
            console.log(textStatus+errorThrown)
            alert("查询失败请稍后再试:"+errorThrown)
        }
    });

    $('body').on('click', 'button', function () {

        var send = {
            "type": "", "jobId": $(this).parent().attr('jobId')
        };
        if ($(this).hasClass('active'))   //上线
        {
            send.type = 'active'
        }
        else if ($(this).hasClass('stop')) {
            send.type = 'stop'
        }
        else if ($(this).hasClass('delete')) {
            send.type = 'delete'
        }
        else if ($(this).hasClass('refresh_all')) {
            send = {"type": "refresh_all"};
        } else {
            return;
        }

        $.ajax({
            type: 'post',
            url: url,
            contentType: "application/json;charset=UTF-8",
            async: false,
            data: JSON.stringify(send),
            success: function (data) {
                window.location.reload()
            }
        });
    });

    /*点击编辑跳转页面*/
    $(document).on("click", ".btn_edit", function () {
        var id = $(this).attr("data-id");
        var type = $(this).attr("data-type");
        if (type == 'flink_sql') {
            window.location.href = "edit.html?type=edit&jobId=" + id;
        } else {
            window.location.href = "etl.html?jobId=" + id;
        }

    });

});
