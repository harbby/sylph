import React, { useState, useEffect, useCallback, useRef } from 'react';
import MonacoEditor from 'react-monaco-editor';
import { PageContainer } from '@ant-design/pro-layout';
import { Layout, Modal, Form, Input, Button, Select } from 'antd';
import { initGraph, initOrgChart, initShapes } from './graph';
import styles from './index.less';

const { Option } = Select;
const { Header, Sider, Content } = Layout;

const options = {
  selectOnLineNumbers: true
};

const layout = {
  labelCol: { span: 6 },
  wrapperCol: { span: 14 }
};

const taskTypes = [
  "Flink Streaming",
  "Spark Streaming",
]

export default (): React.ReactNode => {

  const [code] = useState<string>('');
  const sideRef = useRef<any>();
  const outlineContainerRef = useRef<any>();
  const [graph, setGraph] = useState<any>(null);
  const [modelVisible, setModelVisible] = useState<boolean>(false);
  const containerRef = useCallback((node: HTMLDivElement) => {
    if (node) {
      setGraph(initGraph(node, () => {
        setModelVisible(true)
      }))
    }
  }, []);

  useEffect(() => {
    if (graph) {
      initShapes(sideRef.current, graph)
      initOrgChart(outlineContainerRef.current, graph)
    }
  }, [graph]);

  return (
    <PageContainer>
      <Modal
        title="Basic Modal"
        visible={modelVisible}
        width={1000}
        onOk={() => setModelVisible(false)}
        onCancel={() => setModelVisible(false)}
      >
        <Form
          {...layout}
          // layout="inline"
          style={{width: "100%"}}
        >
          <Form.Item
            label="任务名称"
            name="taskName"
            rules={[{ required: true, message: '请输入任务名称!' }]}
          >
            <Input />
          </Form.Item>

          <Form.Item
            label="任务类别"
            name="taskType"
            rules={[{ required: true, message: '请选择任务类别!' }]}
          >
            <Select>
              {taskTypes.map(taskType => (
                <Option value= {taskType}key={taskType}>{taskType}</Option>
              ))}
            </Select>
          </Form.Item>
        </Form>
        
        <MonacoEditor
          width="100%"
          height="400px"
          language="sql"
          theme="vs-dark"
          value={code}
          options={options}
        />
      </Modal>
      <Layout>
        <Header className={styles.header}>
          <Button type="primary" onClick={() => setModelVisible(true)}>Open Model</Button>
        </Header>
        <Layout>
          <Sider className={styles.leftSide}>
            <div ref={sideRef} className={styles.side}/>
          </Sider>
          <Content className={styles.content}>
            <div ref={containerRef} className={styles.container}/>
          </Content>
          <Sider className={styles.rightSide} width="300">
            <div ref={outlineContainerRef} className={styles.outlineContainer}/>
          </Sider>
        </Layout>
      </Layout>
    </PageContainer>
  )
}