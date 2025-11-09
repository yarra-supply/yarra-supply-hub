import { http } from './http';
import type { ProductSyncRunsPage } from '@/types/productRunRecord';
import type { ProductSyncChunksPage } from '@/types/productChunkRecord';

export type ProductSyncRunsQuery = {
  page: number;
  page_size: number;
};

export type ProductSyncChunksQuery = ProductSyncRunsQuery & {
  run_id?: string;
};

export async function fetchProductSyncRuns(params: ProductSyncRunsQuery) {
  const { data } = await http.get<ProductSyncRunsPage>('/product-sync-records/runs', {
    params,
  });
  return data;
}

export async function fetchProductSyncChunks(params: ProductSyncChunksQuery) {
  const { data } = await http.get<ProductSyncChunksPage>('/product-sync-records/chunks', {
    params,
  });
  return data;
}
