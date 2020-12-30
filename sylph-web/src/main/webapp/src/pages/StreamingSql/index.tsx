/* eslint-disable */
import React from "react";
import {
  LoadingOutlined,
  SettingOutlined,
  FileOutlined,
  SaveOutlined
} from '@ant-design/icons';
import { PageContainer } from '@ant-design/pro-layout';
import { Drawer, Table, notification, Input, Select, Tag, Alert, Button, Row, Col } from "antd";
import { message } from 'antd';
import { EditableCell, EditableFormRow } from './EditableTable';
import { UnControlled as CodeMirror } from 'react-codemirror2'
import 'codemirror/lib/codemirror.css';
import 'codemirror/mode/sql/sql';
import 'codemirror/theme/neo.css';
import styles from './index.less';


export default class StreamingSql extends React.Component {
    state = {
        create: false,
        jobId: null,
        jobName: null,
        engine: "StreamSql",
        query: "create input table xxx()",
        config: {},
        editConfig: {},
        showErrorMessage: "",
        visible: false,
        saveing: false
    };

    columns = [
        {
            title: 'key',
            dataIndex: 'key'
        },
        {
            title: 'value',
            dataIndex: 'value',
            editable: true
        },
    ];

    showDrawer = () => {
        this.setState({
            visible: true,
        });
    };

    constructor(props, context) {
        super()

        this.state.jobId = props.match.params.jobId;
        if (props.location.state !== undefined) {
            this.state.jobName = props.location.state.jobName;
        }
        this.state.create = this.state.jobName !== undefined && this.state.jobName !== null;
    }

    async fetchGetData(url) {
        let result = await fetch(url, { method: "GET" });
        result = await result.json();
        this.setState({ jobName: result.jobName, query: result.queryText, engine: result.type, config: JSON.parse(result.config), editConfig: JSON.parse(result.config) })
    }

    componentWillMount() {
        if (this.state.jobId !== undefined && !this.state.create) {
            this.fetchGetData(`/_sys/job_manger/job/${this.state.jobId}`)
        }
    }

    openNotificationWithIcon = (type, message, description) => {
        notification[type]({
            message: message,
            description: description,
            duration: 6
        });
    };

    async jobSave() {
        this.setState({ saveing: true });
        let result = await fetch("/_sys/job_manger/save", {
            method: "POST",
            body: JSON.stringify({
                id: this.state.jobId,
                jobName: this.state.jobName,
                queryText: this.state.query,
                type: this.state.engine,
                config: JSON.stringify(this.state.config)
            }),
            headers: {
                "content-type": "application/json"
            }
        });
        try {
            result = await result.json();
            if (result.success === false) {
                this.setState({ showErrorMessage: result.message })
                return;
            }
            message.success(`Save job ${this.state.jobName} success`, 5);
        } finally {
            this.setState({ saveing: false });
        }
    }


    render = () => {
        const { Option } = Select;

        const getErrorMessage = () => {
            if (!this.state.showErrorMessage) return;
            return (
                <Alert
                    message={"Error"}
                    description={<pre>{this.state.showErrorMessage}</pre>}
                    type={"error"}
                    showIcon
                    closable
                    onClose={() => this.setState({ showErrorMessage: '' })}
                />
            )
        }

        const saveingIcon = () => {
            if (!this.state.saveing) {
                return;
            }
            return (<LoadingOutlined/>)
        }

        const components = {
            body: {
                row: EditableFormRow,
                cell: EditableCell,
            },
        };
        const columns = this.columns.map(col => {
            if (!col.editable) {
                return col;
            }
            return {
                ...col,
                onCell: record => ({
                    record,
                    editable: col.editable,
                    dataIndex: col.dataIndex,
                    title: col.title,
                    handleSave: (e) => {
                        this.state.editConfig[e.key] = e.value
                        this.setState({ editConfig: this.state.editConfig })
                    },
                }),
            };
        });
        return (
          <PageContainer>
            <div style={{ height: "100%", width: "100%"}}>
                {getErrorMessage()}
                <Row style={{ margin: "10px" }}>
                    <Col span={4} >
                        <Tag style={{ fontSize: "16px", padding: "5px 25px" }} color="blue">Job: {this.state.jobName}</Tag>
                    </Col>
                    <Col span={20} style={{ textAlign: 'right' }}>
                        <Select style={{ margin: "0 10px" }} defaultValue="StreamSql" value={this.state.engine} onSelect={(e) => { this.setState({ engine: e }) }}>
                            <Option value="StreamSql">FlinkStreamSql</Option>
                            <Option value="SparkStreamingSql">SparkStreamingSql</Option>
                            <Option value="StructuredStreamingSql">StructuredStreamingSql</Option>
                        </Select>
                        <Button type="primary" icon={<SettingOutlined/>} onClick={this.showDrawer}>Setting</Button>
                        <Button style={{ margin: "0 10px" }} type="primary" icon={<FileOutlined />} >Files</Button>
                        <Button type="primary" icon={<SaveOutlined />} onClick={() => this.jobSave()}>Save{saveingIcon()}</Button>
                    </Col>
                </Row>
                <CodeMirror
                    value={this.state.query}
                    className={styles.codeMirrorDiv}
                    options={{
                        lineNumbers: true,                     //显示行号  
                        mode: { name: "text/x-sql" },          //定义mode  
                        extraKeys: { "Ctrl": "autocomplete" },   //自动提示配置  
                        theme: "neo"        //material or ambiance         //选中的theme  
                    }}
                    onChange={(editor, data, value) => {
                        this.state.query = value;
                    }}
                />
                <div>
                    <Drawer
                        title="Setting job config"
                        width={"50%"}
                        onClose={()=>{
                            this.setState({
                                visible: false,
                                editConfig: this.state.config
                            });
                        }}
                        visible={this.state.visible}
                    >
                        {/* <EditableTable dataSource={this.state.config}></EditableTable> */}
                        <p>basic configuration:</p>
                        <Table components={components} scroll={{ y: 420 }} pagination={{ pageSize: 50 }} dataSource={(() => {
                            let map = this.state.editConfig
                            return Object.keys(map).map(key => { return { key: key, value: map[key], description: "" } });
                        })()
                        } columns={columns} />
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
                            <Button onClick={() => {
                                this.setState({
                                    visible: false,
                                    editConfig: this.state.config
                                });
                            }} style={{ marginRight: 8 }}>Cancel</Button>
                            <Button onClick={() => {
                                this.setState({
                                    visible: false,
                                    config: this.state.editConfig
                                });
                            }} type="primary">Save</Button>
                        </div>
                    </Drawer>
                </div>
            </div>
          </PageContainer>
        );
    };
}