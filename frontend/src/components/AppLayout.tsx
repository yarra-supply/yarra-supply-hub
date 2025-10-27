
import { useMemo, useState, useEffect } from 'react';
import { useNavigate, useLocation, Outlet } from 'react-router-dom';
import { ConfigProvider, Layout, Menu } from 'antd';
import type { MenuProps } from 'antd'; 
import { AppstoreOutlined, DatabaseOutlined, LoginOutlined, 
  HomeOutlined, ClockCircleOutlined, TruckOutlined, ShopOutlined, 
  SettingOutlined, CloudDownloadOutlined,
} from '@ant-design/icons';

import PageHeader from './PageHeader';
import { useQuery } from '@tanstack/react-query';     
import { me } from '../api/auth';

const { Header, Sider, Content } = Layout;

// 父菜单使用“非路由”的 key，避免与 /products 冲突
const PRODUCT_GROUP_KEY = 'menu-product';
const DOWNLOAD_GROUP_KEY = 'menu-download';      // ← 新增：Download 父菜单的 key


export default function AppLayout() {
  const nav = useNavigate();
  const loc = useLocation();

  // 获取当前用户，用于判断是否显示“Login”菜单: 已登录 → 隐藏“Login”菜单；未登录 → 显示
  const { data: currentUser } = useQuery({
    queryKey: ['auth','me'],
    queryFn: me,
    staleTime: 60_000,
    retry: false,
  });


  // 选中项：优先子菜单路径，再匹配父级
  const selected = useMemo(() => {
    if (loc.pathname === '/dashboard') return ['/dashboard'];
    if (loc.pathname.startsWith('/products')) return ['/products'];
    if (loc.pathname.startsWith('/sync-runs')) return ['/sync-runs'];
    if (loc.pathname.startsWith('/schedules')) return ['/schedules'];

    if (loc.pathname.startsWith('/freight-results')) return ['/freight-results'];
    if (loc.pathname.startsWith('/freight-config')) return ['/freight-config'];

    if (loc.pathname.startsWith('/download')) return ['/download/kogan'];
    
    if (loc.pathname.startsWith('/shopify-jobs')) return ['/shopify-jobs'];
    return ['dashboard'];
  }, [loc.pathname]);


  // 展开态：父菜单使用“非路由”的 key：'PRODUCT_GROUP_KEY' 在 /products 或 /sync-runs 下自动展开
  const shouldOpenProduct = loc.pathname.startsWith('/products') || loc.pathname.startsWith('/sync-runs');
  const shouldOpenDownload = loc.pathname.startsWith('/download');

  const [openKeys, setOpenKeys] = useState<string[]>(
    [
      ...(shouldOpenProduct ? [PRODUCT_GROUP_KEY] : []),
      ...(shouldOpenDownload ? [DOWNLOAD_GROUP_KEY] : []),
    ]
  );

  useEffect(() => {
    setOpenKeys([
      ...(shouldOpenProduct ? [PRODUCT_GROUP_KEY] : []),
      ...(shouldOpenDownload ? [DOWNLOAD_GROUP_KEY] : []),
    ]);
  }, [shouldOpenProduct, shouldOpenDownload]);


  // 构造一个“可变的”菜单数组；最后按需插入 Login
  const menuItems: MenuProps['items'] = [
    { key: '/dashboard', label: 'Dashboard', icon: <HomeOutlined /> },
    { key: '/schedules', label: 'Schedules', icon: <ClockCircleOutlined /> },
    
    {
      key: 'menu-product',
      label: 'Product',
      icon: <AppstoreOutlined />,
      children: [
        { key: '/products', label: 'Products List', icon: <DatabaseOutlined />},
        { key: '/sync-runs', label: 'Product Sync Runs', icon: <DatabaseOutlined /> },
      ],
    },
    
    { key: '/freight-results', label: 'Freight Results', icon: <TruckOutlined /> },
    { key: '/freight-config', label: 'Freight Param Config', icon: <SettingOutlined /> },
    {
      key: 'menu-download',
      label: 'Download',
      icon: <CloudDownloadOutlined />,
      children: [
        { key: '/download/kogan-template', label: 'Kogan Template', icon: <CloudDownloadOutlined /> },
      ],
    },
    { key: '/shopify-jobs', label: 'Shopify Jobs', icon: <ShopOutlined /> },
  ];
  
  // 只有“未登录”才显示 Login 菜单项
  if (!currentUser) {
    menuItems.push({ key: '/login', label: 'Login', icon: <LoginOutlined /> });
  }



  return (
    <Layout style={{ minHeight: '100vh' }}>

      <Sider breakpoint="lg" width={220} collapsedWidth="0" style={{ background: '#3a3d42ff' }} >

        {/* 品牌位：改成 className 方便换字体 */}
        {/* <div className="app-brand">Yarra Supply Hub</div> */}
        <div style={{ color:'#fff', padding:16, fontWeight:700, fontSize:18 }}>Yarra Supply Hub</div>
        
        {/* <ConfigProvider
        theme={{
          components: {
            Layout: {
              siderBg: '#3a3d42ff',           // ← 左侧整块改成灰色（深灰示例）
            },
            Menu: {
              // Menu 处于 dark 模式时，优先使用下面这两个“dark”背景：
              darkItemBg: '#3a3d42ff',         // 普通项背景（要与 siderBg 协同）
              darkSubMenuItemBg: '3a3d42ff',     // 子菜单背景
              itemColor: '#3a3d42ff',          // 常规文字色（未选中、未悬停）
              // 悬停时
              itemHoverBg: '#163058',
              itemHoverColor: '#ffffff',
              // 选中时
              itemSelectedBg: '#1b3a6b',
              itemSelectedColor: '#ffffff',
              itemBorderRadius: 8,               // 圆角（可选，仅视觉，不影响宽度）
            },
          },
        }}
        > */}

        <ConfigProvider
          theme={{
            components: {
              Layout: {
                siderBg: "#3a3d42ff",
              },
              Menu: {
                darkItemBg: "#3a3d42ff",
                darkSubMenuItemBg: "#3a3d42ff",
                itemColor: "#ffffffb3",
                itemHoverBg: "#163058",
                itemHoverColor: "#ffffff",
                itemSelectedBg: "#1b3a6b",
                itemSelectedColor: "#ffffff",
                itemBorderRadius: 8,
              },
            },
          }}
        >

        <Menu
          theme="dark" mode="inline" selectedKeys={selected} openKeys={openKeys}
          onOpenChange={setOpenKeys}
          onClick={({ key }) => {
              // 仅对子菜单（路由路径）做导航；点击父菜单只负责展开/收起
              if (typeof key === 'string' && key.startsWith('/')) nav(key);
          }}
          items={menuItems}
        />
        </ConfigProvider>
      </Sider>

      <Layout>
        <Header style={{ background:'#fff', padding:0 }}>
          <PageHeader />
        </Header>

        <Content style={{ margin: '16px' }}>
          <div style={{ padding: 16, background:'#fff', borderRadius: 8 }}>
             <Outlet />   {/* ← 关键：渲染子路由 */}
          </div>
        </Content>
      </Layout>
    </Layout>

  );
}
