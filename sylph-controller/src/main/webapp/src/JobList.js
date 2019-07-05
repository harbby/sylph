import React from "react";
import { Table, Modal, notification, Tag, Divider, Button, Popconfirm, Icon, Input } from "antd";

export default class JobList extends React.Component {
  deploys = {}

  state = {
    jobList: [],

    columns: [
      {
        title: "Job",
        dataIndex: "jobName",
        key: 'jobName',
      },
      {
        title: "runId",
        dataIndex: "runId",
        key: 'runId',
        width: 200,
        render: (runId, record, index) => {
          if (record.status === 'DEPLOYING') {
            let setJobListItem = (item) => Object.assign([], this.state.jobList, item);
            if (!this.deploys[record.jobId]) {
              var intervalId = setInterval(async () => {
                var result = await this.fetchData(`/job/${record.jobId}`)

                if (result.status !== "DEPLOYING") {
                  clearInterval(this.deploys[record.jobId])
                  this.setState({ jobList: setJobListItem({ [index]: { ...result } }) });
                  delete this.deploys[record.jobId]
                } else {
                  this.setState({ jobList: setJobListItem({ [index]: { ...result } }) });
                }
              }, 1000)
              this.deploys[record.jobId] = intervalId;
            }

            return (
              <Tag color={"blue"} key={runId}>
                <Icon type="loading" />
                &nbsp;&nbsp;processing...
              </Tag>
            );
          }

          if (runId && runId.length > 0) {
            return (
              <Tag color={"blue"} key={runId} onClick={() => window.open(record.appUrl)}>
                {runId}
              </Tag>
            );
          }
          return <Tag color={"red"}>{"暂无"}</Tag>;
        }
      },
      {
        title: "type",
        dataIndex: "type",
        key: 'type',
      },
      {
        title: "status",
        dataIndex: "status",
        key: 'status',
      },
      {
        title: "Action",
        key: "action",
        render: (text, record, index) => {
          let DeployBtn = (
            <Popconfirm
              title="Are you sure deploy this job?"
              onConfirm={() =>
                this.handleDeployOrStop('deploy', record.jobId)
              }
              okText="Yes"
              cancelText="No"
              placement="left"
            >
              <a href="/#">Deploy</a>
            </Popconfirm >
          );
          let StopBtn = (
            <Popconfirm
              title="Are you sure stop this job?"
              onConfirm={() => {
                this.handleDeployOrStop('stop', record.jobId);
              }}
              okText="Yes"
              cancelText="No"
              placement="left"
            >
              <a href="/#">Stop</a>
            </Popconfirm>
          );
          return (
            <span>
              {record.status === "STOP" ? DeployBtn : StopBtn}
              < Divider type="vertical" />

              <Popconfirm
                title="Are you sure Delete this job?"
                onConfirm={() => {
                  this.handleDeployOrStop('delete', record.jobId);
                }}
                okText="Yes"
                cancelText="No"
                placement="left"
              >
                <a href="/#" style={{ color: "red" }}>Delete</a>
              </Popconfirm>
              <Divider type="vertical" />
              <a onClick={() => {
                var toLink;
                var type = record.type;
                if (type === 'StreamSql' || type === 'FlinkMainClass' || type === 'StructuredStreamingSql' || type === 'SparkStreamingSql') {
                  toLink = `/streamingSql/${record.jobId}`;
                } else {
                  toLink = `/streamingEtl/${record.jobId}`;
                }
                this.props.history.push({ pathname: toLink, state: {} });
              }}>Edit</a>
            </span>
          );
        }
      }
    ]
  };

  openNotificationWithIcon = (type, message, description) => {
    notification[type]({
      message: message,
      description: description,
      duration: 6
    });
  };

  async fetchData(path) {
    let result = await fetch(`/_sys/job_manger${path}`, { method: "GET" });
    result = await result.json();

    if (result.success === false) {
      this.openNotificationWithIcon('error', result.error_code, result.message)
      return;
    }
    return result;
  }

  async handleDeployOrStop(action, jobId) {
    await this.fetchData(`/${action}/${jobId}`);
    await this.loadjobs();
  }

  async loadjobs() {
    var result = await this.fetchData("/jobs");
    result && this.setState({ jobList: result });
  }



  componentWillMount() {
    this.loadjobs();
  }

  render = () => {
    return (
      <div>
        <div style={{ textAlign: "right", margin: "20px 10px" }}>
          <Button
            type="primary"
            icon="reload"
            onClick={() => this.loadjobs()}
          >
            Refresh
          </Button>
          <Button style={{ margin: "0 10px" }} type="primary" icon="folder-add" onClick={() => {
            this.props.history.push("/streamingEtl", { data: {} });
          }}>
            Create_ETL
          </Button>
          <Button type="primary" icon="folder-add" onClick={() => this.setState({ visible: true })}>
            Create_StreamSQL
          </Button>
          <Modal
            title="Create New StreamSQL"
            visible={this.state.visible}
            onOk={() => {
              this.setState({ visible: false });
              var jobName = this.refs.create_new_job_id.state.value
              this.props.history.push({
                pathname: `/streamingSql`,
                state: { jobName: jobName, create: true }
              });
            }}
            onCancel={() => { this.setState({ visible: false }); }}
          >
            JobId: <Input placeholder="please enter job id" ref="create_new_job_id"></Input>
          </Modal>
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
