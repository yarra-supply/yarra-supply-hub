import { useMemo, useState } from 'react';
import { Badge, Button, Form, Input, Space, Table, Typography } from 'antd';
import type { ColumnsType, TablePaginationConfig } from 'antd/es/table';
import dayjs from 'dayjs';
import { keepPreviousData, useQuery } from '@tanstack/react-query';
import { fetchProductSyncChunks } from '@/api/productRecord';
import type { ProductSyncChunk } from '@/types/productChunkRecord';

const { Title, Text } = Typography;

const badgeStatusMap: Record<string, 'success' | 'processing' | 'error' | 'default' | 'warning'> = {
  pending: 'default',
  running: 'processing',
  succeeded: 'success',
  completed: 'success',
  failed: 'error',
};

const formatTime = (value?: string | null) => (value ? dayjs(value).format('YYYY-MM-DD HH:mm') : '-');

const renderStatus = (status: string) => (
  <Badge status={badgeStatusMap[status] ?? 'default'} text={status || '-'} />
);

export default function ProductChunkPage() {
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [runId, setRunId] = useState<string | undefined>();

  const [form] = Form.useForm();

  const params = useMemo(
    () => ({ page, page_size: pageSize, run_id: runId || undefined }),
    [page, pageSize, runId],
  );

  const { data, isLoading } = useQuery({
    queryKey: ['product-sync-chunks', params],
    queryFn: () => fetchProductSyncChunks(params),
    placeholderData: keepPreviousData,
  });

  const onSearch = () => {
    const raw = form.getFieldValue('run_id') as string | undefined;
    const cleaned = raw?.trim();
    setPage(1);
    setRunId(cleaned || undefined);
  };

  const onReset = () => {
    form.resetFields();
    setRunId(undefined);
    setPage(1);
  };

  const columns: ColumnsType<ProductSyncChunk> = [
    // { title: 'Chunk ID', dataIndex: 'id', width: 100 },
    {
      title: 'Run ID',
      dataIndex: 'run_id',
      width: 210,
      render: (value: string) => <div style={{ wordBreak: 'break-all', whiteSpace: 'normal' }}>{value}</div>,
    },
    { title: 'Chunk #', dataIndex: 'chunk_idx', width: 100 },
    { title: 'Status', dataIndex: 'status', width: 120, render: renderStatus },
    { title: 'SKU Count', dataIndex: 'sku_count', width: 110 },
    { title: 'Missing SKU Count', dataIndex: 'dsz_missing', width: 110 },
    // { title: 'Failed Batches', dataIndex: 'dsz_failed_batches', width: 150 },
    { title: 'Failed SKUs', dataIndex: 'dsz_failed_skus', width: 130 },
    { title: 'DSZ Requested', dataIndex: 'dsz_requested_total', width: 150 },
    { title: 'DSZ Returned', dataIndex: 'dsz_returned_total', width: 150 },
    {
      title: 'DSZ missing sku',
      dataIndex: 'dsz_missing_sku_list',
      width: 220,
      render: (value: string) => <div style={{ wordBreak: 'break-all', whiteSpace: 'normal' }}>{value}</div>,
    },
    {
      title: 'DSZ failed sku',
      dataIndex: 'dsz_failed_sku_list',
      width: 220,
      render: (value: string) => <div style={{ wordBreak: 'break-all', whiteSpace: 'normal' }}>{value}</div>,
    },
    { title: 'Started At', dataIndex: 'started_at', width: 170, render: (v) => formatTime(v) },
    { title: 'Finished At', dataIndex: 'finished_at', width: 170, render: (v) => formatTime(v) },
    { title: 'Last Error', dataIndex: 'last_error', ellipsis: true },
  ];

  const pagination: TablePaginationConfig = {
    current: page,
    pageSize,
    total: data?.total || 0,
    showSizeChanger: true,
    onChange: (p, ps) => {
      setPage(p);
      setPageSize(ps);
    },
    showTotal: (t) => `Total ${t} chunks`,
  };

  return (
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      <div>
        <Title level={4} style={{ marginBottom: 8 }}>Product Sync Chunks</Title>
        <Text type="secondary">
          {/* 先在表单中输入 run_id（可从 Product Sync Runs 页面复制）以筛选对应分片；不填则展示所有分片。 */}
          Enter a run_id in the form (you can copy it from the [Product Sync Runs] page) to filter the related shards. If left blank, all shards will be shown.
        </Text>
      </div>

      <Form form={form} layout="inline" onFinish={onSearch}>
        <Form.Item name="run_id" label="Run ID">
          <Input placeholder="Paste run UUID" allowClear style={{ width: 320 }} />
        </Form.Item>
        <Form.Item>
          <Space>
            <Button type="primary" htmlType="submit">Search</Button>
            <Button onClick={onReset}>Reset</Button>
          </Space>
        </Form.Item>
      </Form>

      <Table<ProductSyncChunk>
        rowKey="id"
        loading={isLoading}
        columns={columns}
        dataSource={data?.items || []}
        pagination={pagination}
        scroll={{ x: 1400 }}
      />
    </Space>
  );
}
