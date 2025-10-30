

// 下载 Kogan 模版相关类型

export type CountryType = "AU" | "NZ";

export interface KoganExportJobSummary {
  job_id: string;
  file_name: string;
  row_count: number;
  country_type: CountryType;
  status: string;
  exported_at?: string | null;
  applied_at?: string | null;
  created_by?: number | null;
  applied_by?: number | null;
}

export interface KoganExportApplyResult {
  job_id: string;
  status: string;
  applied_at: string | null;
}

export interface KoganExportNoDirtyResponse {
  detail: "no_dirty_sku";
  last_job?: KoganExportJobSummary | null;
}
