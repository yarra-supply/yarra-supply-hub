import { useState } from 'react';
import { Badge, Table } from 'antd';
import type { ColumnsType, TablePaginationConfig } from 'antd/es/table';
import dayjs from 'dayjs';
import { keepPreviousData, useQuery } from '@tanstack/react-query';
import { fetchSyncRuns } from '@/api/syncRuns';
import type { SyncRun } from '@/types/syncRun';

export default function SyncRunsPage() {
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);


  const { data, isLoading } = useQuery({
    queryKey: ['sync-runs', page, pageSize],
    queryFn: () => fetchSyncRuns(page, pageSize),
    placeholderData: keepPreviousData,
  });


  const columns: ColumnsType<SyncRun> = [
    { title: 'Run ID', dataIndex: 'id', width: 220 },
    { title: 'Status', dataIndex: 'status', width: 120,
      render: (s: SyncRun['status']) => {
        const map = { running: 'processing', succeeded: 'success', failed: 'error' } as const;
        return <Badge status={map[s]} text={s} />;
      }
    },
    { title: 'Started', dataIndex: 'started_at', width: 180,
      render: v => dayjs(v).format('YYYY-MM-DD HH:mm') },
    { title: 'Finished', dataIndex: 'finished_at', width: 180,
      render: v => v ? dayjs(v).format('YYYY-MM-DD HH:mm') : '-' },
    { title: 'Total SKU', dataIndex: 'total_sku', width: 120 },
    { title: 'Changed SKU', dataIndex: 'changed_sku', width: 130 },
    { title: 'Note', dataIndex: 'note', ellipsis: true },
  ];


  const pagination: TablePaginationConfig = {
    current: page,
    pageSize,
    total: data?.total || 0,
    showSizeChanger: true,
    onChange: (p, ps) => { setPage(p); setPageSize(ps); },
  };

  
  return (
    <Table<SyncRun>
      rowKey="id"
      loading={isLoading}
      columns={columns}
      dataSource={data?.items || []}
      pagination={pagination}
      scroll={{ x: 1000 }}
    />
  );
}
