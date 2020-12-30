/* eslint-disable */
import React from "react";
import { Table, Tag } from "antd";

export default class ConnectorList extends React.Component {
    state = {
        connectors: []
    };

    columns = [
        {
            title: 'connector',
            dataIndex: 'name',
            key: 'name',
            render: (name, record) => {
                return (<Tag key={name} onClick={() => {
                    debugger
                }}>{name}</Tag>)
            }
        },
        {
            title: 'driver',
            dataIndex: 'driver',
            key: 'driver',
        },
        {
            title: 'type',
            dataIndex: 'type',
            key: 'type',
            render: (type, record) => {
                return <Tag color={"blue"} key={type}>{type}</Tag>
            }
        },
        {
            title: 'realTime',
            dataIndex: 'realTime',
            key: 'realTime'
        }
    ];


    async fetchData() {
        let result = await fetch("/_sys/plugin/list_connectors", {
            method: "GET"
        });

        result = await result.json();
        if (Object.values(result).length === 0) {
            return;
        }
        this.setState({
            connectors: [].concat.call(...(Object.values(result)))
        });
    }

    componentWillMount() {
        this.fetchData()
    }

    render = () => {
        return (
            <div style={{width: '100%', height: '100%'}}>
                <Table dataSource={this.state.connectors} columns={this.columns} expandedRowRender={record => {
                    debugger;
                    return (<p style={{ margin: 0 }}>{JSON.stringify(record.config)}</p>);
                }} />
            </div>
        );
    };
}