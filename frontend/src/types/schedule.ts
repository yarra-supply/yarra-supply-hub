
export type ScheduleKey = 'price_reset' | 'product_full_sync';

export type Dow =
  | 'MON' | 'TUE' | 'WED' | 'THU' | 'FRI' | 'SAT' | 'SUN';

export type Schedule = {
  key: ScheduleKey;
  label?: string;               // 服务端可选返回，前端已做兜底
  enabled: boolean;             // 是否启用

  /** 周几执行：与页面上的选择*/
  day_of_week: Dow;   
  hour: number;                 // 0-23
  minute: number;               // 0-59

  every_2_weeks: boolean;       // 是否每两周（默认 true）
  timezone: string;             // 例如 'Australia/Sydney'
  updated_at?: string;
};
