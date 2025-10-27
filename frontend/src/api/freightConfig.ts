
import { http } from './http';
import type { FreightConfig } from '@/types/freightConfig';

const PREFIX = '/freight-config';


export async function getFreightConfig(): Promise<FreightConfig> {

  // const res = await http.get<FreightConfig>(PREFIX, {
  //   headers: { 'Cache-Control': 'no-cache' },
  // });
  const res = await http.get(PREFIX, { params: { t: Date.now() } }); // 用时间戳避免缓存即可
  console.log('[API] ←', res.status, res.data);

  // 后端已返回扁平 JSON（Pydantic 模型）
  return res.data;                             
}



export async function updateFreightConfig(patch: Partial<FreightConfig>): Promise<FreightConfig> {

  // const res = await http.put<FreightConfig>(PREFIX, payload, {
  //   headers: { 'Cache-Control': 'no-cache' },
  // });
  const res = await http.patch(PREFIX, patch);
  return res.data;
}