import React, { useState } from 'react';
import { PageContainer } from '@ant-design/pro-layout';
import MonacoEditor from 'react-monaco-editor';

const options = {
  selectOnLineNumbers: true
};

export default (): React.ReactNode => {

  const [code] = useState<string>('');
  return (
    <PageContainer>
        <MonacoEditor
          width="100%"
          height="100%"
          language="sql"
          theme="vs-dark"
          value={code}
          options={options}
        />
    </PageContainer>
  )
}