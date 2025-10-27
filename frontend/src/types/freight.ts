


export interface FreightResult {
  id: string;
  sku_code: string;
  zone?: string | null;
  shipping_type?: string;   // 0, 1, 10, 15, 20, Extra2/3/4/5 等
  adjust?: number | null;  // 调整费（派生）
  same_shipping?: number | null;
  shipping_ave?: number | null;
  shipping_ave_m?: number | null;
  shipping_ave_r?: number | null;
  shipping_med?: number | null;
  shipping_med_dif?: number | null;
  rural_ave?: number | null;
  weighted_ave_s?: number | null;
  cubic_weight?: number | null;   // 体积重
  remote_check?: boolean | null;  // 1 表示偏远不送（9999）
  cost?: number | null;
  selling_price?: number | null;
  shopify_price?: number | null;
  kogan_au_price?: number | null;
  kogan_k1_price?: number | null;
  kogan_nz_price?: number | null;
  tags?: string[] | null; // 多值
  updated_at?: string | null;
}


export interface FreightResultPage {
  items: FreightResult[];
  total: number;
}