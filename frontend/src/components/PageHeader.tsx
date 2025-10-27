import { Space, Typography } from 'antd';
import UserMenu from './UserMenu';



export default function PageHeader() {
  return (
    <div style={{ display:'flex', justifyContent:'space-between', alignItems:'center', padding:'0 16px' }}>
      <Space size="small">
        <Typography.Title level={5} style={{ margin: 0 }}> </Typography.Title>
      </Space>
      {/* 右侧可放环境/用户信息 */}
      <Space size="middle">
        <span style={{ color:'#999' }}>User</span>
        <UserMenu /> {/* 右上角头像下拉 */}
      </Space>
    </div>
  );
}
