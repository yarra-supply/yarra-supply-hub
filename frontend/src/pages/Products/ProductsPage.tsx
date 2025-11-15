import { useMemo, useState } from 'react';
import { Button, Form, Input, Space, Table, Tag as AntTag, Select } from 'antd';
import type { ColumnsType, TablePaginationConfig } from 'antd/es/table';
import dayjs from 'dayjs';
import { useQuery } from '@tanstack/react-query';
import { fetchProducts, fetchProductTags, exportProductsCsv } from '@/api/products';
import type { ProductQuery } from '@/api/products';
import type { Product } from '@/types/product';


export default function ProductsPage() {
  const [form] = Form.useForm();

  // ✅ 用 useWatch 监听表单字段，避免未绑定时读取导致的 warning
  // const sku = Form.useWatch('sku', form);
  // const tag = Form.useWatch('tag', form);
  // const tagsSel: string[] = Form.useWatch('tags', form) || [];
  // ✅ 只在点击 Search 时才更新的查询条件
  const [criteria, setCriteria] = useState<{ sku?: string; tags?: string[] }>({});

  // 分页
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);


  // ✅ 仅依赖 criteria + 分页；输入过程中不会变
  const params = useMemo<ProductQuery>(() => {
    return {
      page, page_size: pageSize,
      sku: criteria.sku || undefined,
      tag: criteria.tags && criteria.tags.length ? criteria.tags.join(',') : undefined,
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [page, pageSize, criteria]);


  //从后端获取标签下拉
  const { data: tagList } = useQuery({
    queryKey: ['product-tags'],
    queryFn: fetchProductTags,   // 需要在 src/api/products.ts 里写这个函数
    staleTime: 5 * 60 * 1000,
  });

  // 查询后端商品列表接口
  const { data, isLoading, refetch } = useQuery({
    queryKey: ['products', params],
    queryFn: () => fetchProducts(params),
  });


  const columns: ColumnsType<Product> = [
    { title: 'SKU', dataIndex: 'sku_code', width: 160, fixed: 'left' },
    // { title: 'Title', dataIndex: 'title', ellipsis: true },
    { title: 'Brand', dataIndex: 'brand', width: 90 },
    {
      title: 'Tags',
      dataIndex: 'tags',
      width: 200,
      render: (arr?: string[]) =>
        arr && arr.length ? (
          <Space wrap>
            {arr.map((t) => (
              <AntTag key={t}>{t}</AntTag>
            ))}
          </Space>
        ) : (
          '-'
        ),
    },
    { title: 'Stock', dataIndex: 'stock_qty', width: 90 },
    // { title: 'Status', dataIndex: 'status', width: 110,
    //   render: (v) => <AntTag color={v==='active'?'green':'default'}>{v || '-'}</AntTag>
    // },

    { title: 'Price', dataIndex: 'price', width: 100 },
    { title: 'RRP', dataIndex: 'rrp_price', width: 100 },
    { title: 'Special Price', dataIndex: 'special_price', width: 100 },
    { title: 'Special End', dataIndex: 'special_price_end_date', width: 120,
      render: (v) => v ? dayjs(v).format('YYYY-MM-DD') : '-' },
    { title: 'Shopify Price', dataIndex: 'shopify_price', width: 110 },

    { title: 'Weight/kg', dataIndex: 'weight', width: 100 },
    { title: 'Length/cm', dataIndex: 'length', width: 100 },
    { title: 'Width/cm', dataIndex: 'width', width: 100 },
    { title: 'Height/cm)', dataIndex: 'height', width: 100 },
    { title: 'CBM', dataIndex: 'cbm', width: 100 },
    { title: 'supplier', dataIndex: 'supplier', width: 100 },
    { title: 'ean code', dataIndex: 'ean_code', width: 100 },

    { title: 'Freight (ACT)', dataIndex: 'freight_act', width: 100 },
    { title: 'Freight (NSW_M)', dataIndex: 'freight_nsw_m', width: 100 },
    { title: 'Freight (NSW_R)', dataIndex: 'freight_nsw_r', width: 100 },
    { title: 'Freight (NT_M)', dataIndex: 'freight_nt_m', width: 100 },
    { title: 'Freight (NT_R)', dataIndex: 'freight_nt_r', width: 100 },
    { title: 'Freight (QLD_M)', dataIndex: 'freight_qld_m', width: 100 },
    { title: 'Freight (QLD_R)', dataIndex: 'freight_qld_r', width: 100 },
    { title: 'Freight (Remote)', dataIndex: 'remote', width: 100 },
    { title: 'Freight (SA_M)', dataIndex: 'freight_sa_m', width: 100 },
    { title: 'Freight (SA_R)', dataIndex: 'freight_sa_r', width: 100 },
    { title: 'Freight (TAS_M)', dataIndex: 'freight_tas_m', width: 100 },
    { title: 'Freight (TAS_R)', dataIndex: 'freight_tas_r', width: 100 },
    { title: 'Freight (VIC_M)', dataIndex: 'freight_vic_m', width: 100 },
    { title: 'Freight (VIC_R)', dataIndex: 'freight_vic_r', width: 100 },
    { title: 'Freight (WA_M)', dataIndex: 'freight_wa_m', width: 100 },
    { title: 'Freight (WA_R)', dataIndex: 'freight_wa_r', width: 100 },
    { title: 'Freight (NZ)', dataIndex: 'freight_nz', width: 100 },
    { title: 'attrs hash', dataIndex: 'attrs_hash_current', width: 120 },
    { title: 'Updated', dataIndex: 'updated_at', width: 160,
      render: (v) => v ? dayjs(v).format('YYYY-MM-DD HH:mm') : '-' },
  ];


  // ✅ 只有点击 Search 才更新 criteria -> 触发请求
  const onSearch = () => {
    const { sku, tags } = form.getFieldsValue();
    setPage(1);
    setCriteria({
      sku: sku || undefined,
      tags: tags?.length ? tags : undefined,
    });
    refetch();
  };

  const [downloading, setDownloading] = useState(false);
  
  const handleDownload = async () => {
    try {
      setDownloading(true);
      // 只按“最近一次 Search”的条件（criteria），行为与运费页一致
      const p = {
        sku: criteria.sku || undefined,
        tag: (criteria.tags && criteria.tags.length) ? criteria.tags.join(',') : undefined,
      };
      await exportProductsCsv(p);
    } finally {
      setDownloading(false);
    }
  };


  // ✅ 新增：当 SKU 和 Tags 都为空时，自动恢复到“无筛选”并触发一次查询
  const onFormValuesChange = (_changed: any, all: { sku?: string; tags?: string[] }) => {
    const emptySku = !all.sku || all.sku.trim() === '';
    const emptyTags = !all.tags || all.tags.length === 0;

    if (emptySku && emptyTags) {
      // 只有当当前 criteria 不是“无筛选”时才触发，避免重复请求
      const criteriaIsEmpty = !criteria.sku && !(criteria.tags && criteria.tags.length);
      if (!criteriaIsEmpty) {
        setPage(1);
        setCriteria({});
        refetch();
      }
    }
  };

  const pagination: TablePaginationConfig = {
    current: page,
    pageSize,
    total: data?.total || 0,
    showSizeChanger: true,
    onChange: (p, ps) => { 
      setPage(p); 
      setPageSize(ps); },
    showTotal: (t, [s,e]) => `Total ${t} items · ${s}-${e}`,
  };

  return (
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      <Form form={form} layout="inline" onFinish={onSearch} onValuesChange={onFormValuesChange}>
        <Form.Item name="sku" label="SKU">
          <Input placeholder="e.g. V952-XXXX" allowClear style={{ width: 220 }} />
        </Form.Item>

        {/* 下拉选项 */}
        <Form.Item name="tags" label="Tags">
          <Select
            mode="multiple"
            allowClear
            placeholder="选择一个或多个标签"
            options={(tagList || []).map(t => ({ label: t, value: t }))}
            style={{ width: 320 }}
            maxTagCount="responsive"
            showSearch
          />
        </Form.Item>

        {/* <Form.Item name="tag" label="Tag">
          <Input placeholder="e.g. DropShippingZone" allowClear style={{ width: 220 }} />
        </Form.Item> */}

        <Form.Item>
          <Space>
            <Button type="primary" htmlType="submit">Search</Button>
            <Button onClick={() => { 
              form.resetFields(); 
              setPage(1); 
              setCriteria({});
              refetch();
              }}>
                Reset
            </Button>
            {/* 下载按钮 */}
            <Button onClick={handleDownload} loading={downloading} className="btn-download-grey">
              Download CSV
              </Button>
          </Space>
        </Form.Item>
      </Form>

      <Table<Product>
        rowKey="id"
        loading={isLoading}
        columns={columns}
        dataSource={data?.items || []}
        pagination={pagination}
        scroll={{ x: 1100 }}
      />
    </Space>
  );
}
