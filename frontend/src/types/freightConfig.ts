// 与后端 FreightCalcConfig 的扁平字段一一对应
// 数值类型均为 number，后端 Numeric 可能返回字符串，前端统一转成 number 使用


export interface FreightConfig {
  adjust_threshold: number;
  adjust_rate: number;

  // Remote 哨兵
  remote_1: number;
  remote_2: number;
  wa_r: number;

  // 权重 计算 WeightedAveS
  weighted_ave_shipping_weights: number;
  weighted_ave_rural_weights: number;

  // 体积重
  cubic_factor: number;
  cubic_headroom: number;

  // ShippingType thresholds
  price_ratio: number;
  med_dif_10: number;
  med_dif_20: number;
  med_dif_40: number;
  same_shipping_0: number;
  same_shipping_10: number;
  same_shipping_20: number;
  same_shipping_30: number;
  same_shipping_50: number;
  same_shipping_100: number;

  // Shopify
  shopify_threshold: number;
  shopify_config1: number;
  shopify_config2: number;

  // Kogan AU
  kogan_au_normal_low_denom: number;
  kogan_au_normal_high_denom: number;
  kogan_au_extra5_discount: number;
  kogan_au_vic_half_factor: number;

  // K1
  k1_threshold: number;
  k1_discount_multiplier: number;
  k1_otherwise_minus: number;

  // Kogan NZ
  kogan_nz_service_no: number;
  kogan_nz_config1: number;
  kogan_nz_config2: number;
  kogan_nz_config3: number;

  weight_calc_divisor: number;
  weight_tolerance_ratio: number;
}
