import React from "react";
import { Route } from "react-router-dom";
import { Layout } from "antd";
import Menu from "./Menu";
import JobList from "./JobList";

const { Content } = Layout;

export default () => {
  return (
    <Layout style={{ minHeight: "100vh" }}>
      <Menu />
      <Layout>
        <Content style={{ margin: "16px 16px", background: "white" }}>
          <Route path="/joblist" component={JobList} />
          <Route path="/abc" component={() => <span>AAAAAA</span>} />
        </Content>
      </Layout>
    </Layout>
  );
};
