import React from "react";
import { Layout, Menu, Icon } from "antd";
import { Link } from "react-router-dom";

const { Sider } = Layout;
const { SubMenu } = Menu;

export default class SideMenu extends React.Component {
  state = {
    collapsed: false
  };

  onCollapse = collapsed => {
    console.log(collapsed);
    this.setState({ collapsed });
  };

  render = () => (
    <Sider
      collapsible
      collapsed={this.state.collapsed}
      onCollapse={this.onCollapse}
    >
      <div
        className="logo"
        style={{
          color: "white",
          width: "100%",
          textAlign: "center",
          fontSize: "20px",
          margin: "15px 0"
        }}
      >
        Sylph
      </div>
      <Menu theme="dark" defaultSelectedKeys={["1"]} mode="inline">
        <Menu.Item key="1">
          <Link to="/joblist">
            <Icon type="tool" />
            JobManager
          </Link>
        </Menu.Item>

        <SubMenu
          key="2"
          title={
            <span>
              <Icon type="tool" />
              <span>Connector</span>
            </span>
          }
        >
          <Menu.Item key="2">
            <Link to="/connectors">
              <Icon type="tool" />
              Connectors
          </Link>
          </Menu.Item>
          <Menu.Item key="3">
            <Link to="/connector_manager">
              <Icon type="tool" />
              Manager
          </Link>
          </Menu.Item>
        </SubMenu>


        <Menu.Item key="5">
          <Link to="/serverLogs">
            <Icon type="tool" />
            ServerLogs
          </Link>
        </Menu.Item>
        <SubMenu
          key="sub1"
          title={
            <span>
              <Icon type="user" />
              <span>User</span>
            </span>
          }
        >
          <Menu.Item key="7">
            <span onClick={() => { this.props.logout() }}>
              <Icon type="tool" />
              Logout
            </span>
          </Menu.Item>
        </SubMenu>
      </Menu>
    </Sider>
  );
}
