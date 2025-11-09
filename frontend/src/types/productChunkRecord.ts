
export interface ProductSyncChunk {
  // id: number;
  run_id: string;
  chunk_idx: number;
  status: string;
  // sku_codes: Array<string | Record<string, unknown>>;
  sku_count: number;
  dsz_missing: number;
  dsz_failed_batches: number;
  dsz_failed_skus: number;
  dsz_requested_total: number;
  dsz_returned_total: number;
  dsz_missing_sku_list: string[];
  dsz_failed_sku_list: string[];
  dsz_extra_sku_list: string[];
  started_at?: string | null;
  finished_at?: string | null;
  last_error?: string | null;
  // created_at?: string;
  // updated_at?: string;
}

export interface ProductSyncChunksPage {
  items: ProductSyncChunk[];
  total: number;
}
