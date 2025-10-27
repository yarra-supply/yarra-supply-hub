
import { http } from './http'
import type { FreightResult } from '@/types/freight'
import dayjs from "dayjs";



export type FreightQuery = {
  page: number;
  page_size: number;
  sku?: string;
  tag?: string;
  shippingType?: string; //这边用的是 camelCase，后端记得兼容一下
};


/* 获取 运费计算结果列表 */
export async function fetchFreightResults(q: FreightQuery)
: Promise<{ items: FreightResult[]; total: number }> {
    // api: GET /api/v1/freight-results
    const { data } = await http.get('/freight/results', { params: q });
    return data;
}

// 使用范型，以后你一旦在列里写了不存在的字段，编译就会报错，避免“静默失配”
// export async function fetchFreightResults(params: any) {
//   const res = await http.get<FreightPage>('/freight/results', { params });
//   return res.data;
// }


/* 获取 ShippingType列表 */
export async function fetchShippingTypes(): Promise<string[]> {
  // api: GET /freight/shipping-types
  const { data } = await http.get('/freight/shipping-types');
  return data;
}


/* 导出 CSV：返回 Blob，由调用方负责触发下载 */
export async function exportFreightCSV(params: {
  sku?: string;
  tag?: string;           // 多选 tags -> 逗号分隔传给后端
  shippingType?: string;  // 多选 shippingType -> 逗号分隔传给后端（已兼容）
}) {

  const res = await http.get("/freight/results/export", {
    params,
    responseType: "blob",
  });

  // 如果后端返回的是错误 JSON（有些网关把 4xx 变成 200+JSON）
  const ct = (res.headers["content-type"] || "") as string;
  if (res.status >= 400 || ct.includes("application/json")) {
    const text = await new Response(res.data).text();
    throw new Error(text || `Export failed (${res.status})`);
  }

  // 解析后端文件名（优先使用）
  const cd = (res.headers["content-disposition"] || "") as string;
  const m = cd.match(/filename\*?=(?:UTF-8'')?"?([^\";]+)"?/i);
  const serverName = m?.[1] ? decodeURIComponent(m[1]) : undefined;

  // 兜底：前端自定义文件名（可按你的习惯拼筛选条件/时间）
  const fallback = `freight_results_${dayjs().format("YYYYMMDD_HHmm")}.csv`;
  const filename = serverName || fallback;

  const blob = new Blob([res.data], { type: ct || "text/csv" });
  const href = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = href;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(href);
  
  // GET /freight/results/export 直接跳转的下载方法
  // return http.get('/freight/results/export', {
  //   params,
  //   responseType: 'blob',  // ← CSV 是二进制流，必须 blob；列表返回 JSON 不需要 blob
  // });
}