import React from "react";
import { Drawer, Input, Select, Tag, Alert, Button, Icon, Row, Col } from "antd";
import SyntaxHighlighter from 'react-syntax-highlighter';
import { docco } from 'react-syntax-highlighter/dist/esm/styles/hljs';
import { message } from 'antd';

export default class StreamingSql extends React.Component {
    state = {
        create: false,
        jobId: null,
        engine: "StreamSql",
        query: "",
        config: "{}",
        showErrorMessage: "",
        visible: false,
        saveing: false
    };

    showDrawer = () => {
        this.setState({
            visible: true,
        });
    };

    onClose = () => {
        this.setState({
            visible: false,
        });
    };

    constructor(props, context) {
        super()
        console.log(props)
        this.state.jobId = props.location.state.data.jobId
        this.state.create = props.location.state.data.create
    }

    async fetchGetData(url, prems) {
        url = url + "?rd=" + Math.random();
        for (var i in prems) {
            url += "&" + i + "=" + prems[i];
        }

        let result = await fetch(url, { method: "GET" });
        result = await result.json();
        this.setState({ query: result.query, jobType: result.jobType, config: JSON.stringify(result.config.config) })
    }

    componentDidMount() {
    }

    componentWillMount() {
        if (this.state.jobId !== undefined && !this.state.create) {
            this.fetchGetData("/_sys/stream_sql/get", { jobId: this.state.jobId })
        }
    }

    onEditChange(e) {
        this.setState({ query: e.target.value })
    }

    async save() {
        this.setState({ saveing: true })
        var formData = new FormData();
        formData.set("jobId", this.state.jobId)
        formData.set("query", this.state.query)
        formData.set("jobType", this.state.engine)
        formData.set("config", this.state.config)
        let result = await fetch("/_sys/stream_sql/save", {
            method: "POST",
            body: formData
        });
        result = await result.json()
        this.setState({ saveing: false })
        if (result.status === 'ok') {
            message.success(`Save job ${this.state.jobId} success`, 5);
        } else {
            this.setState({ showErrorMessage: result.msg })
        }
    }


    render = () => {
        const { TextArea } = Input;
        const { Option } = Select;

        const onClose = () => {
            this.setState({ showErrorMessage: '' })
        };

        const getErrorMessage = () => {
            if (!this.state.showErrorMessage) return;
            return (
                <Alert
                    message={""}
                    description={<pre>{this.state.showErrorMessage}</pre>}
                    type={"error"}
                    closable
                    onClose={onClose}
                />
            )
        }

        const saveingIcon = () => {
            if (!this.state.saveing) {
                return;
            }
            return (<Icon type="loading" />)
        }

        return (
            <div>
                {getErrorMessage()}
                <Row style={{ margin: "10px" }}>
                    <Col span={4} >
                        <Tag style={{ fontSize: "16px", padding: "5px 25px" }} color="blue">Job: {this.state.jobId}</Tag>
                    </Col>
                    <Col span={20} style={{ textAlign: 'right' }}>
                        <Select style={{ margin: "0 10px" }} defaultValue="StreamSql" onSelect={(e) => { this.setState({ engine: e }) }}>
                            <Option value="StreamSql">FlinkStreamSql</Option>
                            <Option value="SparkStreamingSql">SparkStreamingSql</Option>
                            <Option value="StructuredStreamingSql">StructuredStreamingSql</Option>
                        </Select>
                        <Button type="primary" icon="setting" onClick={this.showDrawer}>Setting</Button>
                        <Button style={{ margin: "0 10px" }} type="primary" icon="file" >Files</Button>
                        <Button type="primary" icon="save" onClick={() => this.save()}>Save{saveingIcon()}</Button>
                    </Col>
                </Row>
                <TextArea autosize={{ minRows: 10, maxRows: 25 }} value={this.state.query} onChange={this.onEditChange.bind(this)} />
                <div ref={node => this.node = node}>
                    <SyntaxHighlighter language="sql" style={docco} PreTag="pre">
                        {this.state.query}
                    </SyntaxHighlighter>
                </div>

                <div>
                    <Drawer
                        title="Setting job config"
                        width={"50%"}
                        onClose={this.onClose}
                        visible={this.state.visible}
                    >
                        <Input.TextArea rows={20} placeholder="please enter job config" value={this.state.config} onChange={e => {
                            this.setState({ config: e.target.value })
                        }} />
                        <div
                            style={{
                                position: 'absolute',
                                left: 0,
                                bottom: 0,
                                width: '100%',
                                borderTop: '1px solid #e9e9e9',
                                padding: '10px 16px',
                                background: '#fff',
                                textAlign: 'right',
                            }}
                        >
                            <Button onClick={this.onClose} style={{ marginRight: 8 }}>Cancel</Button>
                            <Button onClick={this.onClose} type="primary">Save</Button>
                        </div>
                    </Drawer>
                </div>
            </div>
        );
    };
}