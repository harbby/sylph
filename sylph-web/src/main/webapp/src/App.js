import React from "react";
import { Route } from "react-router-dom";
import { Modal, Layout } from "antd";
import Menu from "./Menu";
import { WrappedNormalLoginForm } from "./Login";
import JobList from "./JobList";
import ServerLog from "./ServerLog";
import StreamingSql from "./StreamingSql";
import StreamingEtl from "./StreamingEtl";
import ConnectorList from "./ConnectorList";
import ConnectorManager from "./ConnectorManager";


const { Content } = Layout;

export default class App extends React.Component {

  state = {
    login: false,
    loading: true,
    userName: null
  }

  async login() {
    let result = await fetch("/_sys/auth/login", {
      method: "POST",
      body: JSON.stringify({
        userName: "",
        password: ""
      }),
      headers: {
        "content-type": "application/json"
      }
    });
    result = await result.json();
    if (result.success === false) {
      this.setState({ login: false, loading: false })
    } else {
      this.setState({ userName: result.userName, login: true, loading: false })
    }
  }

  logout() {
    Modal.confirm({
      title: 'Do you Want to Logout this account?',
      content: this.state.userName,
      onOk: async () => {
        let result = await fetch("/_sys/auth/logout", { method: "GET" });
        result = await result.json();
        this.setState({ login: false })
      },
      onCancel: () => {
        console.log('Cancel');
      },
    });
  }

  componentWillMount() {
    this.login()
  }

  render = () => {
    if (this.state.loading) return (<span></span>)
    if (this.state.login !== true) {
      return (<WrappedNormalLoginForm afterLogin={(userName) => this.setState({ userName: userName, login: true })} />);
    }
    return (
      <Layout style={{ minHeight: "100vh" }}>
        <Menu logout={this.logout.bind(this)} />
        <Layout>
          <Content style={{ margin: "16px 16px", background: "white" }}>
            <Route exact path="/" component={JobList} />
            <Route path="/joblist" component={JobList} />
            <Route path="/connectors" component={ConnectorList} />
            <Route path="/connector_manager" component={ConnectorManager} />

            <Route path="/serverLogs" component={ServerLog} />
            <Route path="/streamingSql/:jobId" component={StreamingSql} />
            <Route exact path="/streamingSql" component={StreamingSql} />
            <Route path="/streamingEtl" component={StreamingEtl} />
            <Route path="/abc" component={() => <span>AAAAAA</span>} />
          </Content>
        </Layout>
      </Layout>
    );
  }

}

