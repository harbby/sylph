/**
 * Created by Polar on 2017/12/14.
 */

/*获取URL中的参数值*/
function getUrlParam(paramName) {
    var arrSource = [];
    var paramValue = '';
    //获取url"?"后的查询字符串
    var search_url = location.search;

    if (search_url.indexOf('?') == 0 && search_url.indexOf('=') > 1) {
        arrSource = decodeURI(search_url).substr(1).split("&");
        //遍历键值对
        for (var i = 0; i < arrSource.length; i++) {
            if (arrSource[i].indexOf('=') > 0) {
                if (arrSource[i].split('=')[0].toLowerCase() == paramName.toLowerCase()) {
                    paramValue = arrSource[i].split("=")[1];
                    break;
                }
            }
        }
    }
    return paramValue;
}

/*页面加载*/
$(function () {
    var sql_editor = CodeMirror.fromTextArea(document.getElementById("query"), {
        mode: 'text/x-sql',
        lineNumbers: true,
        styleActiveLine: true,
        matchBrackets: true
    });
    sql_editor.on('change', editor => {
        document.getElementById('query').value = editor.getValue();
        console.log('change up value:'+ editor.getValue());
    });


    /*add or edit*/
    var type = getUrlParam("type");
    if (type === "add") {
        $("input,textarea").val('');
    } else if (type === "edit") {
        $.ajax({
            url: "/_sys/stream_sql/get?jobId=" + getUrlParam("jobId"),
            type: "get",
            dataType: "json",
            data: {},
            cache: false,
            success: function (result) {
                $("input[name=jobId]").val(result.jobId);
                $("select[name=jobType]").val(result.jobType)
                $("textarea[name=query]").val(result.query);
                sql_editor.setValue(result.query);

                var congfigString = "";
                $.each(result.config.config, function (key, value) {
                    congfigString += key + "= " + value + "\n"
                });
                $("textarea[name=config]").val(congfigString);   //JSON.stringify(result.config.config)

                var files = result.files;
                for (var i = 0; i < files.length; i++) {
                    $('#fileList').append(
                        '<div class="file_row" id="file_' + files[i] + '">' +
                        '<input type="hidden" name="selectFile" value="' + files[i] + '" />' +
                        '<i class="fa fa-trash" onclick="deleteFile(this)"></i>' +
                        '<span class="file_name">' + files[i] + '</span>' +
                        '</div>');
                }
            }
        });
    }

    $('#submit').click(function () {
        var formData = new FormData($('form')[0]);
        if(formData.get("jobId")===""){
            alert("Job name cannot be empty");
            return;
        }
        if(formData.get("query")===""){
            alert("Job query cannot be empty");
            return;
        }
        $.ajax({
            url: '/_sys/stream_sql/save',
            type: 'POST',
            cache: false,
            data: formData,
            processData: false,
            contentType: false
        }).done(function (data) {
            if (data.status === "ok") {
                alert("Successfully saved");
                window.location.href = "index.html";
            } else {
                error_show(data.msg)
            }
        }).fail(function (data) {
            alert(data.msg);
        });
    });

    $('input[name=file]').change(function () {
        $('#fileList').children().remove();
        var files = $(this).prop('files');
        for (var i = 0; i < files.length; i++) {
            $('#fileList').append(
                '<div class="file_row" id="file_' + files[i].name + '">' +
                '<input type="hidden" name="selectFile" value="' + files[i].name + '" />' +
                '<i class="fa fa-trash" onclick="deleteFile(this)"></i>' +
                '<span class="file_name">' + files[i].name + '</span>' +
                '</div>');
        }
    });
});

function deleteFile(obj) {
    $(obj).parent().remove();
}

var UploadFilesLayer;

function openUploadFilesLayer() {
    UploadFilesLayer = layer.open({
        type: 1, area: ['500px', '360px'], title: 'File Upload', shade: 0.6, maxmin: false,
        anim: 1, content: $('#upload-files')
    });
}

var editor = CodeMirror.fromTextArea(document.getElementById("config"), {
    mode: 'properties',
    lineNumbers: true,
    styleActiveLine: true,
    matchBrackets: true
});
editor.on('change', editor => {
    document.getElementById('config').value = editor.getValue();
    console.log('change up value:' + editor.getValue());
});
function openConfigSetLayer() {
    var configSetLayer = layer.open({
        type: 1, area: ['500px', '360px'], title: 'Job_Config', shade: 0.6, maxmin: false,
        anim: 1, content: $('#config-set'),
        success: function (layero, index) { //弹窗完成后 进行语法渲染
            editor.setValue(document.getElementById('config').value)
        }
    });
}

function error_show(message) {
    var configSetLayer = layer.open({
        type: 1, area: ['850px', '540px'], title: 'Error', shade: 0.6, maxmin: false,
        anim: 1, content: $('#error_message'),
        success: function (layero, index) { //弹窗完成后 进行语法渲染
            $('#error_message').text(message)
        }
    });
}
