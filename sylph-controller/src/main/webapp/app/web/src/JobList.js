import React from "react";
import { Table, Tag, Divider, Button, Popconfirm, Icon } from "antd";

export default class JobList extends React.Component {
  state = {
    jobList: [],
    loading: false,
    currentJobId: null,
    columns: [
      {
        title: "Job",
        dataIndex: "jobId"
      },
      {
        title: "runId",
        dataIndex: "yarnId",
        width: 200,
        render: (yarnId, record) => {
          if (this.state.loading && this.state.currentJobId === record.jobId) {
            return (
              <Tag color={"blue"} key={yarnId}>
                <Icon type="loading" />
                &nbsp;&nbsp;processing...
              </Tag>
            );
          }
          if (!isNaN(+yarnId)) {
            return (
              <Tag color={"blue"} key={yarnId}>
                {yarnId}
              </Tag>
            );
          }
          return <Tag color={"red"}>{"暂无"}</Tag>;
        }
      },
      {
        title: "type",
        dataIndex: "type"
      },
      {
        title: "status",
        dataIndex: "status"
      },
      {
        title: "Action",
        key: "action",
        render: (text, record) => {
          let DeployBtn = (
            <Popconfirm
              title="Are you sure deploy this job?"
              onConfirm={() => {
                this.handleDeployOrStop({
                  type: "active",
                  jobId: record.jobId
                });
              }}
              okText="Yes"
              cancelText="No"
              placement="left"
            >
              <a href="#">Deploy</a>
            </Popconfirm>
          );
          let StopBtn = (
            <Popconfirm
              title="Are you sure stop this job?"
              onConfirm={() => {
                this.handleDeployOrStop({ type: "stop", jobId: record.jobId });
              }}
              okText="Yes"
              cancelText="No"
              placement="left"
            >
              <a href="#">Stop</a>
            </Popconfirm>
          );
          return (
            <span>
              {record.status === "RUNNING" ? StopBtn : DeployBtn}
              <Divider type="vertical" />
              <a style={{ color: "red" }}>Delete</a>
              <Divider type="vertical" />
              <a>Edit</a>
            </span>
          );
        }
      }
    ]
  };
  async fetchData(postData) {
    let result = await fetch("/_sys/job_manger", {
      method: "POST",
      body: JSON.stringify(postData),
      headers: {
        "content-type": "application/json"
      }
    });
    result = await result.json();
    if (result && result.data)
      this.setState({
        jobList: result.data
      });
  }
  async handleDeployOrStop(requestData) {
    await this.fetchData(requestData);
    await new Promise(resolve => {
      this.setState({ loading: true, currentJobId: requestData.jobId });
      setTimeout(() => {
        this.setState({ loading: false });
        resolve();
      }, 4000);
    });
    await this.fetchData({ type: "list", jobId: "" });
  }
  componentWillMount() {
    this.fetchData({ type: "list", jobId: "" });
  }

  render = () => {
    return (
      <div>
        <div style={{ textAlign: "right", margin: "20px 10px" }}>
          <Button
            type="primary"
            icon="reload"
            onClick={() => this.fetchData({ type: "list", jobId: "" })}
          >
            Refresh
          </Button>
          <Button style={{ margin: "0 10px" }} type="primary" icon="folder-add">
            Create_ETL
          </Button>
          <Button type="primary" icon="folder-add">
            Create_StreamSQL
          </Button>
        </div>
        <Table
          bordered
          pagination={false}
          dataSource={this.state.jobList}
          columns={this.state.columns}
          rowKey={"jobId"}
        />
      </div>
    );
  };
}
