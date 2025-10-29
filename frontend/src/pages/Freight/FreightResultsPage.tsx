
//运费计算结果页面

import { useMemo, useState } from 'react';
import { Button, Form, Input, Select, Space, Table, Tag as AntTag } from 'antd';
import type { ColumnsType, TablePaginationConfig } from 'antd/es/table';
import dayjs from 'dayjs';
import { useQuery } from '@tanstack/react-query';
import { fetchFreightResults, fetchShippingTypes, exportFreightCSV } from '@/api/freight';
import { fetchProductTags } from '@/api/products';
import type { FreightResult } from '@/types/freight';


// const shippingTypeOptions = [
//   { label: 'All', value: '' },
//   { label: '0 (Free)', value: '0' },
//   { label: '1 (Same & Free)', value: '1' },
//   { label: '10', value: '10' },
//   { label: '15', value: '15' },
//   { label: '20', value: '20' },
//   { label: 'Extra2', value: 'Extra2' },
//   { label: 'Extra3', value: 'Extra3' },
//   { label: 'Extra4', value: 'Extra4' },
//   { label: 'Extra5', value: 'Extra5' },
// ];

// 多选下拉里不提供空值（清空即代表“全部”）
// const shippingTypeOptionsMulti = shippingTypeOptions.filter(o => o.value !== '');


export default function FreightResultsPage() {
  const [form] = Form.useForm();

  // 仅点击 Search 时更新 criteria，避免输入即频繁查询
  const [criteria, setCriteria] = useState<{
    sku?: string;
    tags?: string[];
    shippingType?: string[];
  }>({});


  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);
  // 与 ProductsPage 保持一致的监听方式
  // const sku = Form.useWatch('sku', form);
  // const tagsSel: string[] = Form.useWatch('tags', form) || [];
  // const shippingTypeSel: string[] = Form.useWatch('shipping_type', form) || [];

  
  // 产品页的标签接口
  const { data: tagList } = useQuery({
    queryKey: ['product-tags'],
    queryFn: fetchProductTags,
    staleTime: 5 * 60 * 1000,
  });


  // 运费类型接口
  const { data: shippingTypeList } = useQuery({
    queryKey: ['shipping-types'],
    queryFn: fetchShippingTypes,
    staleTime: 5 * 60 * 1000,
  });


  const params = useMemo(() => {
    return {
      page, page_size: pageSize,
      // // 后端接受 `tag` 为逗号分隔的字符串
      // tag: tagsSel.length ? tagsSel.join(',') : undefined,
      // // 后端接受 `shipping_type`(或 shippingType) 为逗号分隔的字符串；后端已做兼容
      // shipping_type: shippingTypeSel.length ? shippingTypeSel.join(',') : undefined,

      sku: criteria.sku || undefined,
      tag: (criteria.tags && criteria.tags.length) ? criteria.tags.join(',') : undefined,
      shippingType: (criteria.shippingType && criteria.shippingType.length)
        ? criteria.shippingType.join(','): undefined,
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [page, pageSize, criteria]);

  
  // 运费计算结果接口
  const { data, isLoading, refetch } = useQuery({
    queryKey: ['freight-results', params],
    queryFn: () => fetchFreightResults(params),
  });


  // Ant Design Table 的列配置。它告诉表格：每一列显示什么标题、到数据对象的哪个字段去取值
  const columns: ColumnsType<FreightResult> = [
    // title：列头显示的文字, 后端 JSON 需要有 "sku_code": "V201-001" 这样的字段。
    // 前端拿 row.shipping_type
    // 浏览器渲染时，Table 直接从接口返回的 JSON 里按 dataIndex 去找同名字段，与 TS 类型无关
    { title: 'SKU', dataIndex: 'sku_code', width: 180, fixed: 'left' },
    {
      title: 'Tags',
      dataIndex: 'tags',
      width: 180,
      render: (arr?: string[], row?: FreightResult) => {
        const list = arr?.length ? arr : row?.tags ?? [];
        return list.length ? list.map(t => <AntTag key={t}>{t}</AntTag>) : '-';
      },
    },
    
    { title: 'Shipping Type', dataIndex: 'shipping_type', width: 120 },
    { title: 'Adjust', dataIndex: 'adjust', width: 100, render: (v) => v == null ? '-' : Number(v).toFixed(2) },
    { title: 'Same Shipping', dataIndex: 'same_shipping', width: 100 },
    { title: 'Ave', dataIndex: 'shipping_ave', width: 100 },
    { title: 'Ave_M', dataIndex: 'shipping_ave_m', width: 100 },
    { title: 'Ave_R', dataIndex: 'shipping_ave_r', width: 100 },
    { title: 'Shipping_Med', dataIndex: 'shipping_med', width: 100 },
    { title: 'Remote_Check', dataIndex: 'remote_check', width: 100, 
      render: (v?: boolean) => v === true ? 'Yes' : v === false ? 'No' : '-' },

    { title: 'Rural_Ave', dataIndex: 'rural_ave', width: 100 },
    { title: 'WeightedAveS', dataIndex: 'weighted_ave_s', width: 100 },
    { title: 'Shipping_Med_Diff', dataIndex: 'shipping_med_dif', width: 120 },

    { title: 'CubicWt', dataIndex: 'cubic_weight', width: 100 },
    { title: 'UpdateWeight', dataIndex: 'weight', width: 100 },
    
    { title: 'Selling Price', dataIndex: 'selling_price', width: 120 },
    { title: 'Shopify Price', dataIndex: 'shopify_price', width: 140 },
    { title: 'Kogan AU Price', dataIndex: 'kogan_au_price', width: 140 },
    { title: 'Kogan K1 Price', dataIndex: 'kogan_k1_price', width: 140 },
    { title: 'Kogan NZ Price', dataIndex: 'kogan_nz_price', width: 140 },
    { title: 'Price Ratio', dataIndex: 'price_ratio', width: 120, render: v => v == null ? '-' : Number(v).toFixed(4) },
    
    { title: 'Updated', dataIndex: 'updated_at', width: 150,
      render: v => v ? dayjs(v).format('YYYY-MM-DD HH:mm') : '-' },
  ];


  const pagination: TablePaginationConfig = {
    current: page,
    pageSize,
    total: data?.total || 0,
    showSizeChanger: true,
    pageSizeOptions: [10, 20, 30, 50],
    onChange: (p, ps) => { setPage(p); setPageSize(ps); },
    showTotal: (t, [s,e]) => `Total ${t} · ${s}-${e}`,
  };


  const onSearch = () => {
    const { sku, tags, shippingType } = form.getFieldsValue();
    setPage(1);
    setCriteria({
      sku: sku || undefined,
      tags: tags?.length ? tags : undefined,
      shippingType: shippingType?.length ? shippingType : undefined,
    });
    refetch();
  };

  const [downloading, setDownloading] = useState(false);
  
  const handleDownload = async () => {
    setDownloading(true);
    try {
      // 用的是 criteria
      const p = {
        sku: criteria.sku || undefined,
        tag: (criteria.tags && criteria.tags.length) ? criteria.tags.join(',') : undefined,
        shippingType: (criteria.shippingType && criteria.shippingType.length)
          ? criteria.shippingType.join(',') : undefined,
      };
      await exportFreightCSV(p);
    } finally {
      setDownloading(false);
    }
  };

  return (
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      <Form form={form} layout="inline" onFinish={onSearch} className="freight-filters">
        <Form.Item name="sku" label="SKU">
          <Input allowClear placeholder="V952-..." style={{ width: 240 }} />
        </Form.Item>

        {/* 下拉选项？ */}
        {/* 与 ProductsPage 一致：从后端拉标签，多选 */}
        <Form.Item name="tags" label="Tags">
          <Select
            mode="multiple"
            allowClear
            placeholder="选择一个或多个标签"
            options={(tagList || []).map((t) => ({ label: t, value: t }))}
            style={{ width: 320 }}
            maxTagCount="responsive"
            showSearch
          />
        </Form.Item>
        {/* <Form.Item name="tag" label="Tag">
          <Input placeholder="e.g. DropShippingZone" allowClear style={{ width: 220 }} />
        </Form.Item> */}


        {/* ShippingType 支持多选；清空=全部 */}
        <Form.Item name="shippingType" label="ShippingType">
          <Select
            mode="multiple"
            allowClear
            placeholder="选择一个或多个 ShippingType"
            options={(shippingTypeList || []).map(v => ({ label: v, value: v }))}
            style={{ width: 300 }}
            maxTagCount="responsive"
          />
        </Form.Item>
        {/* <Form.Item name="shipping_type" label="ShippingType" initialValue="">
          <Select options={shippingTypeOptions} style={{ width: 200 }} />
        </Form.Item> */}

        <Form.Item>
          <Space>
            <Button type="primary" htmlType="submit">
              Search
              </Button>
            <Button onClick={() => { 
              form.resetFields(); 
              setPage(1); 
              setCriteria({});
              refetch();
              }}>Reset</Button>
              {/* NEW: 浅灰色下载按钮 */}
              <Button onClick={handleDownload} loading={downloading} className="btn-download-grey">
                Download CSV
              </Button>
          </Space>
        </Form.Item>
      </Form>

      <Table<FreightResult>
        // rowKey="sku_code"
        rowKey={(r) => r.sku_code + (r.zone || '') + (r.shipping_type || '')}
        loading={isLoading}
        columns={columns}
        dataSource={data?.items || []}
        pagination={pagination}
        scroll={{ x: 1400 }}
      />
    </Space>
  );
}
