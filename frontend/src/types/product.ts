
export interface Product {
  id: string;
  sku_code: string;
  brand?: string;
  stock_qty?: number;
  // status?: 'active' | 'discontinued';

  price?: number;
  rrp_price?: number;
  special_price?: number;
  special_price_end_date?: string;
  shopify_price?: number;


  length?: number;
  width?: number;
  height?: number;
  weight?: number;
  cbm?: number;
  supplier?: string;
  ean_code?: string;

  // freight: Freight;  // 取消嵌套对象，直接扁平字段
  freight_act?: number;
  freight_nsw_m?: number;
  freight_nsw_r?: number;
  freight_nt_m?: number;
  freight_nt_r?: number;
  freight_qld_m?: number;
  freight_qld_r?: number;
  remote?: number;
  freight_sa_m?: number;
  freight_sa_r?: number;
  freight_tas_m?: number;
  freight_tas_r?: number;
  freight_vic_m?: number;
  freight_vic_r?: number;
  freight_wa_m?: number;
  freight_wa_r?: number;
  freight_nz?: number;

  shopify_variant_id?: string;
  attrs_hash_current?: string;
  updated_at?: string;
  tags?: string[];           // 后端对外仍用 tags；底层来自 product_tags
}

export interface ProductsPage { items: Product[]; total: number; }




