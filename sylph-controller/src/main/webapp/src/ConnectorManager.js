import React from "react";
import { Table, message, Row, Col, Modal, Tag, Divider, Button, Popconfirm, Icon, Input } from "antd";

export default class ConnectorManager extends React.Component {
    state = {
        loading: false,
        connector: []
    };

    columns = [
        {
            title: 'name',
            dataIndex: 'name',
            key: 'name',
            render: (name, record) => {
                return (<Tag key={name} onClick={() => {
                }}>{name}</Tag>)
            }
        },
        {
            title: 'size',
            dataIndex: 'size',
            key: 'size'
        },
        {
            title: 'loadTime',
            dataIndex: 'loadTime',
            key: 'loadTime'
        },
        {
            title: 'action',
            dataIndex: 'action',
            key: 'action',
            render: (name, record) => {
                return (<Popconfirm
                    title="Are you sure Delete this moudle?"
                    onConfirm={async () => {
                        this.setState({ loading: true })
                        await fetch(`/_sys/plugin/delete_module/?name=${record.name}`, { method: "GET" })
                        this.fetchData();
                    }}
                    okText="Yes"
                    cancelText="No"
                    placement="left"
                >
                    <a href="#" style={{ color: "red" }}>Delete</a>
                </Popconfirm>)
            }
        }
    ];


    async fetchData() {
        let result = await fetch("/_sys/plugin/list_modules", {
            method: "GET"
        });

        result = await result.json();
        this.setState({
            loading: false,
            connector: result
        });
    }

    componentWillMount() {
        this.fetchData()
    }

    async reloadConnectors() {
        this.setState({ loading: true })
        await fetch("/_sys/plugin/reload", { method: "GET" });
        message.success(`reload success`);
        this.fetchData();
    }

    render = () => {
        const loadingIcon = () => {
            if (!this.state.loading) {
                return;
            }
            return (<Icon type="loading" />)
        }

        return (
            <div>

                <Row style={{ margin: "10px" }}>
                    <Col style={{ textAlign: 'right' }}>
                        <Button type="primary" icon="setting" onClick={this.reloadConnectors.bind(this)}>Reload{loadingIcon()}</Button>
                    </Col>
                </Row>

                <Table dataSource={this.state.connector} columns={this.columns} expandedRowRender={record => {
                    debugger
                    return (<div>
                        {
                            record.drivers.map(x => {
                                return (<Tag key={x} color={"blue"}> {x}</Tag>)
                            })
                        }
                    </div>)
                }} />
            </div >
        );
    };
}