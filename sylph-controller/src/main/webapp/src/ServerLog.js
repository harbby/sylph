import React from "react";
import { Table, Tag, Divider, Button, Popconfirm, Icon } from "antd";
import { AnsiColors } from "./lib/AnsiColors";

export default class ServerLog extends React.Component {
    state = {
        arrLogs: [],
        last_num: -1,
        id: null,
        intervalId: null
    };
    showlog(json) {
        var stickToBottom = true;
        if (json !== "" && json !== null) {
            this.state.id = json.id;
            if (json.logs === null || json.logs.length === 0) {
                return
            }

            var time = new Date().getTime();
            for (let num in json.logs) {
                if (this.state.arrLogs.length > 1000) {
                    this.state.arrLogs.shift()  //删除第一个元素
                }
                this.state.arrLogs.push({ key: time + "_" + num, val: json.logs[num] })
            }
            //debugger;

            let log1 = this.refs.scroll_con;
            if (log1.scrollTop < log1.scrollHeight - log1.clientHeight - 1) {
                stickToBottom = false
                return;
            }
            this.setState({ id: json.id, last_num: json.next, arrLogs: this.state.arrLogs });
            if (stickToBottom) {
                log1.scrollTo(0, log1.scrollHeight)
                //or log1.scrollTop = log1.scrollHeight;  //滚动条在最下面
            }
        }
    }

    async fetchData(url, prems) {
        url = url + "?rd=" + Math.random();
        for (var i in prems) {
            url += "&" + i + "=" + prems[i];
        }

        let result = await fetch(url, { method: "GET" });
        try {
            result = await result.json();
            this.showlog(result)
        } catch (e) {
            console.log(e);
        }
    }

    componentWillMount() {
        this.state.id = null
        var intervalId = setInterval(() => {
            this.fetchData("/_sys/server/logs", {
                last_num: this.state.last_num,
                id: this.state.id
            })
        }, 1000)
        this.setState({ intervalId: intervalId })
    }

    componentWillUnmount() {
        console.log(`Clearing ShowLogs Interval ${this.state.intervalId}`)
        clearInterval(this.state.intervalId)
    }

    render = () => {
        const kleur = require('kleur');

        return (
            <div style={{ height: "95vh", overflow: "scroll" }} ref="scroll_con">
                {
                    this.state.arrLogs.map(log => {
                        return <AnsiColors log={log.val} />
                    })
                }
            </div>
        );
    };
}