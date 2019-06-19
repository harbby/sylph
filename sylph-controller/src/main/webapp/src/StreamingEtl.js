import React from "react";
import { Input, Select, Table, Tag, Divider, Button, Popconfirm, Icon } from "antd";

export default class StreamingSql extends React.Component {
    state = {
        jobId: null,
        graph: ""
    };
    constructor(props, context) {
        super()
        console.log(props)
        this.state.jobId = props.location.state.data.jobId
    }

    async fetchGetData(url, prems) {
        url = url + "?rd=" + Math.random();
        for (var i in prems) {
            url += "&" + i + "=" + prems[i];
        }

        let result = await fetch(url, { method: "GET" });
        result = await result.json();
        this.setState({ graph: result.query, jobType: result.jobType, config: result.config })
    }

    componentWillMount() {
        if (this.state.jobId !== undefined) {
            this.fetchGetData("/_sys/etl_builder/get", { jobId: this.state.jobId })
        }
    }

    onEditChange(e) {
        this.setState({ query: e.target.value })
    }

    render = () => {
        const { TextArea } = Input;
        return (
            <div>
                etl job: {this.state.jobId}
                <p>not support</p>
                <div id={"actuators_select"}></div>
                <div id={"control-panel"}></div>
                <div id={"flow_modal"}></div>
                <div id={"flow-panel"} style={{ height: "95vh", width: "100px" }}></div>
            </div>
        );
    };
}