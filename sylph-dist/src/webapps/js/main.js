/**
 * 添加方块(控件)
 * @param {*} parentId
 * @param {*id} nodeId
 * @param {*} nodeLable
 * @param {*} position
 */
function addNode(parentId, nodeId, nodeLable, position) {
    var panel = d3.select("#" + parentId);
    panel.append('div').style('width', '120px').style('height', '50px')
        .style('position', 'absolute')
        .style('top', position.y).style('left', position.x)
        .style('border', '2px #9DFFCA solid').attr('align', 'center')
        .attr('id', nodeId).classed('node', true)
        .text(nodeLable);

    return jsPlumb.getSelector('#' + nodeId)[0];
}


/**
 * 添加节点(in out)
 * @param {*daw} instance
 * @param {*dwa} node
 * @param {*daw} ports
 * @param {*dwad} type
 */
function addPorts2(instance, node, ports, type) {
    //Assume horizental layout
    var number_of_ports = ports.length;
    var i = 0;
    var height = $(node).height();  //Note, jquery does not include border for height
    var y_offset = 1 / ( number_of_ports + 1);
    var y = 0;

    for (; i < number_of_ports; i++) {
        var anchor = [0, 0, 0, 0];
        var paintStyle = {radius: 5, fillStyle: '#FF8891'};
        var isSource = false, isTarget = false;
        if (type === 'output') {
            anchor[0] = 1;
            paintStyle.fillStyle = '#D4FFD6';
            isSource = true;
        } else {
            isTarget = true;
        }

        anchor[1] = y + y_offset;
        y = anchor[1];
        var uuid=node.getAttribute("id") + "-" + ports[i];
        var endpoint=instance.addEndpoint(node, {
            uuid: uuid,
            paintStyle: paintStyle,
            anchor: anchor,
            maxConnections: -1,
            isSource: isSource,
            isTarget: isTarget
        });
        $(endpoint.canvas).data("uuid",uuid);
    }
}

/**
 * 给方块添加点
 * @param {*} instance
 * @param {*} node
 * @param {*} in_num
 * @param {*} out_num
 */
function addPorts(instance, node, in_num, out_num) {
    var get_ports = function (x, num) {
        var ports = [];
        for (var i = 1; i <= num; i++) {
            ports.push(x + i);
        }
        return ports;
    };
    var out_ports = get_ports('out', out_num);
    var in_ports = get_ports('in', in_num);
    addPorts2(instance, node, out_ports, 'output');
    addPorts2(instance, node, in_ports, 'input');
}

/*
 *
 * 双击修改节点数据
 *
 * */
function doubleClickData(node) {
    $("#" + node).dblclick(function () {
        var self = $(this);
        $("#modal_title").html(self.text());
        $(".modal_textarea").val(self.data("data"));
        $("#flow_modal").modal('show');
        $("#flow_confirm").attr("data-id",self.attr("id"));
    });
}
/*
* 保存节点数据
* */
$("#flow_confirm").click(function(){
    var node_id=$(this).attr("data-id");
    $("#"+node_id).data("data",$(".modal_textarea").val());
    $("#flow_modal").modal('hide');
});

/*
 *
 * 删除节点及其连接线
 *
 * */
function bindDeleteNode(instance, node) {
    $("#flow-panel").on("mouseenter", "#" + node, function () {
        var self = $(this);
        self.append('<img class="node_img" src="img/close2.png" style="" />');
        self.on("click", ".node_img", function () {
            $(".delete_text").html($("#" + node).text());
            $("#delete_modal").modal('show');
            $("#delete_confirm").click(function () {
                //删除连接线
                instance.detachAllConnections(node);
                //删除锚点
                instance.removeAllEndpoints(node);
                //删除节点
                $("#" + node).remove();
                $("#delete_modal").modal('hide');
            })
        });
    });
    $("#flow-panel").on("mouseleave", "#" + node, function () {
        $(this).find("img.node_img").remove();
    });
}

/*
 *
 * 获取所有节点及其连接线
 *
 * */
function getAllNodes(instance) {
    /*获取连接线*/
    var edges = [];
    $.each(instance.getAllConnections(), function (idx, connection) {
        var label = connection.getOverlays(connection.id)[1].getLabel();
        var sourceUuid=$(connection.endpoints[0].canvas).data("uuid");
        var targetUuid=$(connection.endpoints[1].canvas).data("uuid");
        edges.push({
            uuids:[sourceUuid,targetUuid],
            labelText: label
        });
    });
    /*获取节点*/
    var nodes = [];
    $("#flow-panel").find(".node").each(function (idx, element) {
        var elem = $(element);
        nodes.push({
            nodeId: elem.attr("id"),
            nodeText: elem.text(),
            nodeData: elem.data("data"),
            nodeConfig: elem.data("config"),
            nodeX: parseInt(elem.css("left"), 10),
            nodeY: parseInt(elem.css("top"), 10)
        });
    });
    /*返回json*/
    var node_json = {
        edges: edges,
        nodes: nodes
    };
    return node_json;
}
/*
 *
 * 绘制节点及其连接线
 *
 * */
function drawNodesConnections(instance, nodesCon) {
    var edges = nodesCon.edges;
    var nodes = nodesCon.nodes;
    //节点
    for (var i = 0; i < nodes.length; i++) {
        //节点
        var node = addNode('flow-panel', nodes[i].nodeId, nodes[i].nodeText, {
            x: nodes[i].nodeX + 'px',
            y: nodes[i].nodeY + 'px'
        });
        //锚点8
        addPorts(instance, node, nodes[i].nodeConfig.in, nodes[i].nodeConfig.out);
        //节点绑定双击事件
        var currentNode = {
            data: nodes[i].nodeData,
            config: nodes[i].nodeConfig
        };
        $("#" + nodes[i].nodeId).data(currentNode);
        //双击修改
        doubleClickData(nodes[i].nodeId);
        //删除
        bindDeleteNode(instance, nodes[i].nodeId);
        //可拖动
        instance.draggable($(node));
    }
    //连接线
    for (var j = 0; j < edges.length; j++) {
        var connect=instance.connect({
            uuids: edges[j].uuids
        });
        if(typeof connect!=="undefined"){
            connect.getOverlays(connect.id)[1].setLabel(edges[j].labelText);
        }
    }
}


/**
 * 交互式创建节点 控件工具箱(左侧区域的)
 */
function getTreeData() {
    var tree = [
        {
            text: "系统工具箱",
            nodes: [
                {
                    text: "source",
                    data: {
                        a: 1,
                        b: 2
                    },
                    config: {
                        in: 0,
                        out: 1,
                        //是否可拖动
                        drag: 1
                    }
                },
                {
                    text: "transfrom",
                    //携带的数据
                    data: {},
                    //业务定义 kv
                    config: {
                        in: 1,
                        out: 1,
                        //是否可拖动
                        drag: 1
                    }
                },
                {
                    text: "sink",
                    //携带的数据
                    data: {
                        a: 1,
                        b: 2
                    },
                    //业务定义 kv
                    config: {
                        in: 1,
                        out: 0,
                        //是否可拖动
                        drag: 1
                    }
                }
            ]
        },
        {
            text: "用户自定义插件",
            nodes: [
                {
                    text: "某标签1",
                    //携带的数据,
                    data: {},
                    //业务定义 kv
                    config: {
                        in: 1,
                        out: 1,
                        //是否可拖动
                        drag: 1
                    }
                },
                {
                    text: "实时uv",
                    data: {},
                    config: {
                        in: 1,
                        out: 1,
                        //是否可拖动
                        drag: 1
                    }
                }
            ]
        }
    ];

    return tree;
}
/**
 * 交互式创建节点 控件工具箱(左侧区域的)
 */
function getNodeData() {
    var node_json = {
        connection: [{
            connectionId: "con_10",
            sourceId: "node1515581337612",
            targetId: "node1515581338897",
            sourcePoint: "ep_2",
            targetPoint: "ep_6",
            sourceAnchor: '',
            targetAnchor: '',
            labelText: ''
        }],
        nodes: [
            {
                nodeId: "node1515581337612",
                nodeText: "transfrom",
                nodeX: 184,
                nodeY: 137,
                nodeData: '{a:1}',
                nodeConfig: {
                    drag: 1,
                    out: 1,
                    in: 1
                }
            },
            {
                nodeId: "node1515581338897",
                nodeText: "sink",
                nodeX: 607,
                nodeY: 225,
                nodeData: '{c:1}',
                nodeConfig: {
                    out: 1,
                    in: 1
                }
            }
        ]
    };

    return node_json;
}

/*等待DOM和jsPlumb初始化完毕*/
jsPlumb.ready(function () {
    //初始化左侧节点树
    $('#control-panel').treeview(
        {
            data: getTreeData()
        });
    //初始化JsPlumb配置参数
    var color = "#E8C870";
    //创建jsPlumb实例
    var instance = jsPlumb.getInstance({
        Connector: ["Bezier", {curviness: 50}],   //基本连接线类型 使用Bezier曲线
        DragOptions: {cursor: "pointer", zIndex: 2000},
        PaintStyle: {strokeStyle: color, lineWidth: 2},  //线条样式
        EndpointStyle: {radius: 5, fillStyle: color},
        //叠加层
        ConnectionOverlays: [
            ["Arrow", {
                location: 1,
                id: "arrow",
                length: 14,
                foldback: 0.9
            }],
            ["Label", {
                label: "Default", id: "label", cssClass: "aLabel",
                events: {
                    dblclick: function (labelOverlay, originalEvent) {
                        //双击修改文字
                        var self = $(labelOverlay.canvas);
                        var text = self.text();
                        self.html("");
                        self.append("<input type='text' class='label_input_text' value='" + text + "'/>");
                        //enter键确认
                        self.find("input[type='text']").keydown(function () {
                            //获取浏览器
                            var bro = publicData.getBrowser();
                            if (bro == "Firefox") {
                                //火狐浏览器
                                if (e.which == 13) {
                                    labelOverlay.setLabel(self.find("input[type='text']").val());
                                }
                            } else {
                                //其他浏览器
                                if (event.keyCode == 13) {
                                    labelOverlay.setLabel(self.find("input[type='text']").val());
                                }
                            }
                        });
                    }
                }
            }]//这个是鼠标拉出来的线的属性
        ],
        HoverPaintStyle: {strokeStyle: "#7073EB"},
        EndpointHoverStyle: {fillStyle: "#7073EB"},
        Container: "flow-panel"
    });

    /**
     * 拖拽出控件
     */
    $('#flow-panel').on('drop', function (ev) {
        //avoid event conlict for jsPlumb
        if (ev.target.className.indexOf('_jsPlumb') >= 0) {
            return;
        }

        ev.preventDefault();
        var mx = '' + ev.originalEvent.offsetX + 'px';
        var my = '' + ev.originalEvent.offsetY + 'px';

        var text = ev.originalEvent.dataTransfer.getData('text'); //文本
        var data = ev.originalEvent.dataTransfer.getData('data'); //携带的内容(json字符串)
        var config = JSON.parse(ev.originalEvent.dataTransfer.getData('config')); //业务定义

        var uid = new Date().getTime();
        var node_id = 'node' + uid;
        //节点
        var node = addNode('flow-panel', node_id, text, {x: mx, y: my});
        //锚点
        addPorts(instance, node, config.in, config.out);
        //节点绑定双击事件
        var currentNode = {
            data: data,
            config: config
        };
        $("#" + node_id).data(currentNode);
        //双击修改
        doubleClickData(node_id);
        //删除
        bindDeleteNode(instance, node_id);
        //在面板中可拖动
        instance.draggable($(node));
    }).on('dragover', function (ev) {
        ev.preventDefault();
        console.log('on drag over');
    });

    instance.doWhileSuspended(function () {

        // declare some common values:
        var arrowCommon = {foldback: 0.8, fillStyle: color, width: 5},
            // use three-arg spec to create two different arrows with the common values:
            overlays = [
                ["Arrow", {location: 0.8}, arrowCommon],
                ["Arrow", {location: 0.2, direction: -1}, arrowCommon]
            ];
        instance.draggable($('.node'));
    });

    //立即生效
    jsPlumb.fire("jsFlowLoaded", instance);


    var job_id = getUrlParam("jobId");
    if (job_id != '') {
        $('#task_name').val(job_id);
        //页面加载获取流程图
        $.ajax({
            url: "/_sys/job_graph_edit/?type=edit&jobId="+job_id,
            type: "post",
            data: {},
            success: function (result) {
                var _result=JSON.parse(result);
                if(_result.graph && _result.graph!=""){
                    drawNodesConnections(instance,_result.graph);
                }
            },
            error: function (result) {
                alert("接口拉取失败");
            }
        });
    }

    /*点击保存*/
    $("#flow_save").click(function () {
        var task=$("#task_name").val();
        if(task==""){
            alert("任务名称不能为空");
            return;
        }
        var formData = new FormData();
        formData.append("jobId", task);
        formData.append("graph", JSON.stringify(getAllNodes(instance)));
        var element=$('#select_file')[0].files;
        for (var i = 0; i < element.length; i++) {
            formData.append('file', element[i]);
        }
        formData.append('config', $('.config_modal_textarea').val());
        $.ajax({
            url: '/_sys/job_graph_edit?type=save',
            type: 'POST',
            cache: false,
            data: formData,
            processData: false,
            contentType: false
        }).done(function(result) {
            var result = JSON.parse(result);
            if (result.status == "ok") {
                alert("保存成功");
                window.location.href = "index.html";
            } else {
                alert(result.msg);
            }
        }).fail(function(data) {
            alert("接口请求失败");
        });
    });

    $('input[name=file]').change(function(){
        $('#fileList').children().remove();
        var files = $(this).prop('files');
        for(var i = 0; i < files.length; i++) {
            $('#fileList').append(
                '<div class="file-row" id="file_'+files[i].name+'">' + files[i].name + '</div>');
        }
    });

});

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



