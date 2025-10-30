
import { http } from "./http";
import type {
  CountryType,
  KoganExportApplyResult,
  KoganExportJobSummary,
  KoganExportNoDirtyResponse,
} from "@/types/koganTemplateDownload";


const CREATE_EXPORT_URL = "/kogan-template/export";
const DOWNLOAD_EXPORT_URL = "/kogan-template/download";
const APPLY_EXPORT_URL = "/kogan-template/export";


/**
 * 创建导出任务，返回 job 元数据。
 */
export async function createKoganTemplateExport(
  country: CountryType,
): Promise<KoganExportJobSummary | KoganExportNoDirtyResponse> {
  const res = await http.post<KoganExportJobSummary | KoganExportNoDirtyResponse>(CREATE_EXPORT_URL, null, {
    params: { country_type: country },
  });
  return res.data;
}


/**
 * 根据 jobId 下载已生成的 CSV 文件。
 */
function parseJobSummaryFromHeaders(
  headers: Record<string, unknown>,
  fallback: Partial<KoganExportJobSummary> = {},
): KoganExportJobSummary {
  const header = (key: string) => {
    const lower = key.toLowerCase();
    const record = headers as Record<string, string | undefined>;
    const direct = record[lower] ?? record[key];
    if (direct !== undefined) {
      return direct;
    }
    const getter = (headers as any).get;
    if (typeof getter === "function") {
      return getter.call(headers, lower) ?? getter.call(headers, key);
    }
    return undefined;
  };

  const jobId = header("x-kogan-export-job") || fallback.job_id || "";
  const rowCountRaw = header("x-kogan-export-rows");
  const rowCount = rowCountRaw ? Number(rowCountRaw) : fallback.row_count ?? 0;
  const countryRaw = header("x-kogan-export-country") || fallback.country_type;
  const status = header("x-kogan-export-status") || fallback.status || "exported";
  const appliedAtRaw = header("x-kogan-export-applied-at") || (fallback.applied_at as string | null | undefined);
  const exportedAtRaw = header("x-kogan-export-exported-at") || (fallback.exported_at as string | null | undefined);
  const appliedAt = appliedAtRaw ? appliedAtRaw : null;
  const exportedAt = exportedAtRaw ? exportedAtRaw : null;

  return {
    job_id: jobId,
    file_name: fallback.file_name || "",
    row_count: rowCount,
    country_type: ((countryRaw || fallback.country_type) as CountryType) || "AU",
    status,
    exported_at: exportedAt,
    applied_at: appliedAt,
    created_by: fallback.created_by ?? null,
    applied_by: fallback.applied_by ?? null,
  };
}


export async function downloadKoganTemplateCSVByJob(
  jobId: string,
  fallbackFileName?: string,
  baseline?: Partial<KoganExportJobSummary>,
): Promise<KoganExportJobSummary> {
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

  const summary = parseJobSummaryFromHeaders(res.headers, {
    job_id: jobId,
    file_name: filename,
    ...baseline,
  });
  return summary;
}


/**
 * 一步操作：先创建导出任务再下载，返回创建好的 job 信息，便于后续重下&确认。
 */
export async function downloadKoganTemplateCSV(
  country: CountryType,
): Promise<KoganExportJobSummary | KoganExportNoDirtyResponse> {
  const job = await createKoganTemplateExport(country);
  if ("detail" in job && job.detail === "no_dirty_sku") {
    return job;
  }
  const summary = await downloadKoganTemplateCSVByJob(job.job_id, job.file_name, job);
  return summary;
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
