
import { http } from "./http";
import type {
  CountryType,
  KoganExportApplyResult,
  KoganExportJobSummary,
} from "@/types/koganTemplateDownload";


const CREATE_EXPORT_URL = "/kogan-template/export";
const DOWNLOAD_EXPORT_URL = "/kogan-template/download";
const APPLY_EXPORT_URL = "/kogan-template/export";


/**
 * 创建导出任务，返回 job 元数据。
 */
export async function createKoganTemplateExport(
  country: CountryType,
): Promise<KoganExportJobSummary> {
  const res = await http.post<KoganExportJobSummary>(CREATE_EXPORT_URL, null, {
    params: { country_type: country },
  });
  return res.data;
}


/**
 * 根据 jobId 下载已生成的 CSV 文件。
 */
export async function downloadKoganTemplateCSVByJob(
  jobId: string,
  fallbackFileName?: string,
): Promise<void> {
  const res = await http.get<Blob>(DOWNLOAD_EXPORT_URL, {
    params: { job_id: jobId },
    responseType: "blob",
  });

  const cd = (res.headers["content-disposition"] || "") as string;
  const match = cd.match(/filename\*?=(?:UTF-8'')?"?([^";]+)"?/i);
  const serverFilename = match?.[1] ? decodeURIComponent(match[1]) : undefined;
  const filename = serverFilename || fallbackFileName || `kogan_export_${jobId}.csv`;

  const contentType = (res.headers["content-type"] as string) || "text/csv";
  const blob = new Blob([res.data], { type: contentType });
  const href = URL.createObjectURL(blob);

  const a = document.createElement("a");
  a.href = href;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(href);
}


/**
 * 一步操作：先创建导出任务再下载，返回创建好的 job 信息，便于后续重下&确认。
 */
export async function downloadKoganTemplateCSV(
  country: CountryType,
): Promise<KoganExportJobSummary> {
  const job = await createKoganTemplateExport(country);
  await downloadKoganTemplateCSVByJob(job.job_id, job.file_name);
  return job;
}


/**
 * 回写导出任务
 */
export async function applyKoganTemplateExport(
  jobId: string,
): Promise<KoganExportApplyResult> {
  const res = await http.post<KoganExportApplyResult>(`${APPLY_EXPORT_URL}/${jobId}/apply`);
  return res.data;
}
