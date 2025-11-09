export interface ProductSyncRun {
  id: string;
  run_type?: string | null;
  status: string;
  shopify_bulk_id?: string | null;
  shopify_bulk_status?: string | null;
  shopify_bulk_url?: string | null;
  total_shopify_skus?: number | null;
  changed_count?: number | null;
  note?: string | null;
  started_at: string;
  finished_at?: string | null;
  webhook_received_at?: string | null;
  created_at: string;
  updated_at: string;
}

export interface ProductSyncRunsPage {
  items: ProductSyncRun[];
  total: number;
}

