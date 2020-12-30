export default [
  {
    path: '/user',
    layout: false,
    routes: [
      {
        name: 'login',
        path: '/user/login',
        component: './user/login',
      },
    ],
  },
  {
    path: '/serverLogs',
    name: 'ServerLogs',
    icon: 'DatabaseOutlined',
    component: './ServerLog',
  },
  {
    path: '/JobManager',
    name: 'JobManager',
    icon: 'DatabaseOutlined',
    component: './JobManager',
  },
  {
    name: 'connectors',
    icon: 'CodeOutlined',
    path: '/connectors',
    routes: [
      {
        path: 'connectors/list',
        name: 'ConnectorList',
        icon: 'CodeOutlined',
        component: './Connectors',
      },
      {
        path: 'connectors/manager',
        name: 'ConnectorManager',
        icon: 'ShareAltOutlined',
        component: './Manager',
      }
    ],
  },
  {
    path: '/editor',
    name: 'editor',
    icon: 'CodeOutlined',
    routes: [
      {
        path: '/editor/code-editor',
        name: 'code',
        icon: 'CodeOutlined',
        component: './CodeEditor',
      },
      {
        path: '/editor/graph-editor',
        name: 'graph',
        icon: 'ShareAltOutlined',
        component: './GraphEditor',
      }
    ],
  },
  {
    path: '/streamingSql/:jobId',
    component: './StreamingSql',
  },
  {
    path: '/streamingSql',
    // exact: true,
    component: './StreamingSql',
  },
  {
    path: '/',
    redirect: '/welcome',
  },
  {
    path: '/welcome',
    component: './Welcome',
  },
  {
    component: './404',
  },
];
