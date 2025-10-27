// src/pages/LoginPage.tsx
import React from "react";
import {
  Button, Card, Form, Input, Typography, Space, Divider, message,
} from "antd";
import { UserOutlined, LockOutlined, LoginOutlined } from "@ant-design/icons";
import { login } from "../api/auth";
import { useNavigate } from "react-router-dom";

const { Title, Text } = Typography;

export default function LoginPage() {
  const [loading, setLoading] = React.useState(false);
  const [form] = Form.useForm();
  const navigate = useNavigate();

  const onFinish = async (values: { username: string; password: string }) => {
    setLoading(true);
    try {
      await login(values);
      message.success("Signed in");
      navigate("/", { replace: true });
    } catch (e: any) {
      message.error(e?.response?.data?.detail || "Invalid username or password");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div
      style={{
        minHeight: "100vh",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        padding: 16,
        background: "var(--ant-color-bg-layout, #f5f5f5)",
      }}
    >
      <Card
        style={{ width: 420, borderRadius: 16 }}
        bodyStyle={{ padding: 28 }}
        bordered
      >
        <Space direction="vertical" size={8} style={{ width: "100%" }}>
          <Title level={3} style={{ margin: 0, textAlign: "center" }}>
            Yarra Supply Hub
          </Title>
          <Text type="secondary" style={{ display: "block", textAlign: "center" }}>
            Please sign in to continue
          </Text>
        </Space>

        <Divider style={{ margin: "18px 0 24px" }} />

        <Form
          form={form}
          layout="vertical"
          name="login"
          onFinish={onFinish}
          autoComplete="on"
          requiredMark={false}
          initialValues={{ username: "", password: "" }}
        >
          <Form.Item
            label="Username"
            name="username"
            rules={[
              { required: true, message: "Please enter your username" },
              { min: 2, message: "At least 2 characters" },
            ]}
          >
            <Input
              size="large"
              placeholder="e.g. admin"
              prefix={<UserOutlined />}
              allowClear
              autoFocus
            />
          </Form.Item>

          <Form.Item
            label="Password"
            name="password"
            rules={[{ required: true, message: "Please enter your password" }]}
          >
            <Input.Password
              size="large"
              placeholder="Your password"
              prefix={<LockOutlined />}
              onPressEnter={() => form.submit()}
            />
          </Form.Item>

          <Form.Item style={{ marginTop: 8 }}>
            <Button
              type="primary"
              htmlType="submit"
              size="large"
              icon={<LoginOutlined />}
              block
              loading={loading}
            >
              Sign in
            </Button>
          </Form.Item>
        </Form>

        {/* 开发阶段的小提示，可删 */}
        <div
          style={{
            marginTop: 6,
            padding: "8px 12px",
            borderRadius: 8,
            background: "rgba(24,144,255,0.06)",
          }}
        >
          <Text type="secondary">
            Dev hint: <Text code>yarrasupply / yarrasupply2025</Text>
          </Text>
        </div>

        <Divider style={{ margin: "18px 0 8px" }} />
        <Space direction="vertical" size={0} style={{ width: "100%" }}>
          <Text type="secondary" style={{ fontSize: 12 }}>
            This system is for authorised staff only.
          </Text>
        </Space>
      </Card>
    </div>
  );
}
