import { useMemo, useState } from 'react';
import { Badge, Table, Typography } from 'antd';
import type { ColumnsType, TablePaginationConfig } from 'antd/es/table';
import dayjs from 'dayjs';
import { keepPreviousData, useQuery } from '@tanstack/react-query';
import { fetchProductSyncRuns } from '@/api/productRecord';
import type { ProductSyncRun } from '@/types/productRunRecord';

const { Title } = Typography;

const badgeStatusMap: Record<string, 'success' | 'processing' | 'error' | 'default' | 'warning'> = {
  running: 'processing',
  completed: 'success',
  failed: 'error',
  succeeded: 'success',
  pending: 'default',
};

const formatTime = (value?: string | null) => (value ? dayjs(value).format('YYYY-MM-DD HH:mm') : '-');

const renderStatus = (status: string) => (
  <Badge status={badgeStatusMap[status] ?? 'default'} text={status || '-'} />
);

export default function ProductRunsPage() {
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);

  const params = useMemo(
    () => ({ page, page_size: pageSize }),
    [page, pageSize],
  );

  const { data, isLoading } = useQuery({
    queryKey: ['product-sync-runs', params],
    queryFn: () => fetchProductSyncRuns(params),
    placeholderData: keepPreviousData,
  });

  const columns: ColumnsType<ProductSyncRun> = [
    { title: 'Run ID', dataIndex: 'id', width: 230 },
    { title: 'Type', dataIndex: 'run_type', width: 100, render: (v) => v || '-' },
    { title: 'Status', dataIndex: 'status', width: 120, render: renderStatus },
    {
      title: 'Bulk ID',
      dataIndex: 'shopify_bulk_id',
      width: 220,
      render: (value: string) => <div style={{ wordBreak: 'break-all', whiteSpace: 'normal' }}>{value}</div>,
    },
    { title: 'Bulk Status', dataIndex: 'shopify_bulk_status', width: 130, render: (v) => v || '-' },
    { title: 'Changed SKU', dataIndex: 'changed_count', width: 130 },
    { title: 'Shopify SKUs', dataIndex: 'total_shopify_skus', width: 150 },
    { title: 'Started At', dataIndex: 'started_at', width: 170, render: (v) => formatTime(v) },
    { title: 'Finished At', dataIndex: 'finished_at', width: 170, render: (v) => formatTime(v) },
    { title: 'Webhook Received', dataIndex: 'webhook_received_at', width: 190, render: (v) => formatTime(v) },
    { title: 'Note', dataIndex: 'note', ellipsis: true },
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
    showTotal: (t) => `Total ${t} runs`,
  };

  return (
    <div>
      <Title level={4} style={{ marginBottom: 16 }}>Product Sync Runs</Title>
      <Table<ProductSyncRun>
        rowKey="id"
        loading={isLoading}
        columns={columns}
        dataSource={data?.items || []}
        pagination={pagination}
        scroll={{ x: 1300 }}
      />
    </div>
  );
}
