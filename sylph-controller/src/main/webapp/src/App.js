import React from "react";
import { Route } from "react-router-dom";
import { Layout } from "antd";
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
  }

  async login() {
    let result = await fetch("/_sys/auth/login", {
      method: "POST",
      body: JSON.stringify({
        user: "",
        password: ""
      }),
      headers: {
        "content-type": "application/json"
      }
    });
    result = await result.json();
    this.setState({ loading: false });
    if (result.success === false) {
      this.setState({ login: false })
    } else {
      this.setState({ login: true })
    }
  }

  async logout() {
    let result = await fetch("/_sys/auth/logout", { method: "GET" });
    result = await result.json();
    this.setState({ login: false })
  }

  componentWillMount() {
    this.login()
  }

  render = () => {
    if (this.state.loading) return (<span></span>)
    if (this.state.login !== true) {
      return (<WrappedNormalLoginForm afterLogin={() => this.setState({ login: true })}></WrappedNormalLoginForm>);
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

