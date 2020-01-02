import React from 'react'
import { Form, Icon, Input, Button, Checkbox } from 'antd';

class Login extends React.Component {
    render() {
        let { afterLogin } = this.props;
        const handleSubmit = e => {
            e.preventDefault();
            this.props.form.validateFields(async (err, values) => {
                if (!err) {
                    console.log('Received values of form: ', values);
                }
                let result = await fetch("/_sys/auth/login", {
                    method: "POST",
                    body: JSON.stringify(values),
                    headers: {
                        "content-type": "application/json"
                    }
                });
                result = await result.json();
                if (result.userName === values.userName) {
                    console.log(`${values.user} login ok`)
                    afterLogin(result.userName)
                }
            });
        };


        const { getFieldDecorator } = this.props.form;
        return (
            <Form onSubmit={handleSubmit} style={{ width: "25vw", margin: "10vh auto 0 auto" }}>
                <Form.Item>
                    {getFieldDecorator('userName', {
                        rules: [{ required: true, message: 'Please input your username!' }],
                    })(
                        <Input
                            prefix={<Icon type="user" style={{ color: 'rgba(0,0,0,.25)' }} />}
                            placeholder="Username"
                        />,
                    )}
                </Form.Item>
                <Form.Item>
                    {getFieldDecorator('password', {
                        rules: [{ required: true, message: 'Please input your Password!' }],
                    })(
                        <Input
                            prefix={<Icon type="lock" style={{ color: 'rgba(0,0,0,.25)' }} />}
                            type="password"
                            placeholder="Password"
                        />,
                    )}
                </Form.Item>
                <Form.Item>
                    <div>
                        {getFieldDecorator('remember', {
                            valuePropName: 'checked',
                            initialValue: true,
                        })(<Checkbox>Remember me</Checkbox>)}
                        <a className="login-form-forgot" href="">
                            Forgot password
                    </a>
                    </div>
                    <Button type="primary" htmlType="submit" className="login-form-button">
                        Log in
                    </Button>
                    Or <a href="">register now!</a>
                </Form.Item>
            </Form>
        );
    }
}

export const WrappedNormalLoginForm = Form.create({ name: 'normal_login' })(Login);