
import { http } from "./http";   
import type { CountryType } from "@/types/koganTemplateDownload"



/**
 * 下载“仅变化字段”的 Kogan CSV（AU/NZ）。
 * 后端接口：GET /api/v1/kogan-template/download?country_type=AU|NZ
 * - 相对路径，交给 Vite 代理；加上 responseType: 'blob'
 */
export async function downloadKoganTemplateCSV(country: CountryType): Promise<void> {

  const url = '/kogan-template/download';

  // 1）发起请求，关键是设置 responseType: "blob"，告诉 axios：把响应当作二进制文件，不要当 JSON 解析
  // 相对路径，交给 Vite 代理；加上 responseType: 'blob'
  const res = await http.get(url, {
    params: { country_type: country },
    responseType: "blob",  
  });


  // 2) 解析响应头中的文件名（后端通常会返回 Content-Disposition: attachment; filename="xxx.csv"）
  const cd = (res.headers["content-disposition"] || "") as string;
  const match = cd.match(/filename\*?=(?:UTF-8'')?"?([^\";]+)"?/i);
  const serverFilename = match?.[1] ? decodeURIComponent(match[1]) : undefined;


  // 3) 兜底文件名：如果后端没给 filename，就自己生成一个合理的
  const fallback = `kogan_diff_${country}_${new Date()
    .toISOString()
    .replace(/[-:]/g, "")
    .replace(/\.\d{3}Z$/, "Z")}.csv`;
  const filename = serverFilename || fallback;

  // 6) 把 axios 返回的 blob 数据包装成浏览器可下载的 Blob 对象
  const contentType = (res.headers["content-type"] as string) || "text/csv";
  const blob = new Blob([res.data], { type: contentType });

  // 7) 生成一个临时的 URL（指向内存中的这个 Blob）
  const href = URL.createObjectURL(blob);

  // 8) 创建一个 <a> 标签，设置 download 属性为文件名，然后“程序化点击”触发浏览器下载
  const a = document.createElement("a");
  a.href = href;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();

  // 9) 释放这个临时 URL，避免内存泄漏
  URL.revokeObjectURL(href);
}
