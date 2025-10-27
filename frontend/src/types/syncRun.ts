export type SyncRun = {
  id: string;
  started_at: string;
  finished_at?: string | null;
  total_sku?: number;
  changed_sku?: number;
  status: 'running' | 'succeeded' | 'failed';
  note?: string | null;
};
