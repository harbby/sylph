import React from "react";
import { Route } from "react-router-dom";
import { Layout } from "antd";
import Menu from "./Menu";
import JobList from "./JobList";
import ServerLog from "./ServerLog";
import StreamingSql from "./StreamingSql";
import StreamingEtl from "./StreamingEtl";
import ConnectorList from "./ConnectorList";
import ConnectorManager from "./ConnectorManager";


const { Content } = Layout;

export default () => {

  return (
    <Layout style={{ minHeight: "100vh" }}>
      <Menu />
      <Layout>
        <Content style={{ margin: "16px 16px", background: "white" }}>
          <Route exact path="/" component={JobList} />
          <Route path="/joblist" component={JobList} />
          <Route path="/connectors" component={ConnectorList} />
          <Route path="/connector_manager" component={ConnectorManager} />

          <Route path="/serverLogs" component={ServerLog} />
          <Route path="/streamingSql" component={StreamingSql} />
          <Route path="/streamingEtl" component={StreamingEtl} />
          <Route path="/abc" component={() => <span>AAAAAA</span>} />
        </Content>
      </Layout>
    </Layout>
  );
};
