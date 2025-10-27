// 商品API封装层
//FreightQuery: TypeScript 类型定义，用来描述 前端和后端交互的参数
// FreightResult: TypeScript 类型定义，用来描述 前端和后端返回数据结构


import { http } from './http';
import type { ProductsPage } from '@/types/product';
import dayjs from "dayjs";


export type ProductQuery = {
  page: number;
  page_size: number;
  sku?: string;
  tag?: string;
};


/*
  查询商品列表接口
*/
export async function fetchProducts(params: any) {
  const res = await http.get<ProductsPage>('/products', { params });
  return res.data;
}


/*
  查询商品tags接口
*/
export async function fetchProductTags(): Promise<string[]> {
  const { data } = await http.get('/products/tags');
  return data;
}


/*
  导出产品 CSV（与列表筛选同参）
*/
export async function exportProductsCsv(params: {
  sku?: string;
  tag?: string;        // 多个用逗号拼
}) {

  const res = await http.get("/products/export", {
    params,
    responseType: "blob",
  });

  // 一些网关会把 4xx 包装成 200+JSON，这里兜底判断一下
  const contentType = (res.headers["content-type"] as string) || "";
  if (res.status >= 400 || contentType.includes("application/json")) {
    const text = await new Response(res.data).text();
    throw new Error(text || `Export failed (${res.status})`);
  }

  // 解析后端文件名（Content-Disposition）
  const cd = (res.headers["content-disposition"] || "") as string;
  const m = cd.match(/filename\*?=(?:UTF-8'')?"?([^\";]+)"?/i);
  const serverName = m?.[1] ? decodeURIComponent(m[1]) : undefined;

  // 前端兜底文件名（可按需自定义：把筛选条件拼进去也行）
  const fallback = `products_${dayjs().format("YYYYMMDD_HHmm")}.csv`;
  const filename = serverName || fallback;

  const blob = new Blob([res.data], { type: contentType || "text/csv" });
  const href = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = href;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(href);

  //浏览器直接下载
  // return http.get('/products/export', {
  //   params,
  //   responseType: 'blob',   // 文件下载必须 blob
  // });
}
