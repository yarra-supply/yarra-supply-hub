

// 下载 Kogan 模版相关类型

export type CountryType = "AU" | "NZ";

export interface KoganExportJobSummary {
  job_id: string;
  file_name: string;
  row_count: number;
  country_type: CountryType;
}

export interface KoganExportApplyResult {
  job_id: string;
  status: string;
  applied_at: string | null;
}
