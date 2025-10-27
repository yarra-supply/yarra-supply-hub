
import { http } from './http';
import type { SyncRun } from '../types/syncRun';

export async function fetchSyncRuns(page: number, page_size: number)
: Promise<{items: SyncRun[]; total: number}> {
  const { data } = await http.get('/product-sync-runs', { params: { page, page_size } });
  
  return data;
}
