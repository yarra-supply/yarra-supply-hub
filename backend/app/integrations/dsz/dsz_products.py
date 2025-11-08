

"""
DSZ 产品高层 API（按 SKUs 批量获取）：
   - 接收任意数量的 SKU，内部按 50/批（或配置）拆请求；
   - 支持 GET/POST 两种调用方式（.env 可切）；
   - 每个子批对比“请求 vs 返回”的 SKU 集合，发现缺失/多余会打 warning 日志；
   - 自动合并去重（按 sku 字段）并汇总统计。
"""
from __future__ import annotations
import logging, time
from typing import Any, Dict, Iterable, List, Optional, Tuple

from app.core.config import settings
from app.integrations.dsz.errors import DSZPayloadError
from app.integrations.dsz.http_client import DSZHttpClient
import json
from pprint import pprint

logger = logging.getLogger(__name__)



# -------- 提供给task调用 --------
def get_products_by_skus(skus: Iterable[str]) -> List[dict]:
    """按传入 SKUs 获取 DSZ 产品列表，仅返回合并后的商品数据。"""
    api = DSZProductsAPI()

    # todo 构建自己使用的sku结构题，只保留需要的字段？
    return api.fetch_by_skus(skus, return_stats=False)  # type: ignore[return-value]


"""
  要产品列表 + 汇总统计（requested/returned/missing/extra）。
"""
def get_products_by_skus_with_stats(skus: Iterable[str]) -> Tuple[List[dict], Dict[str, Any]]:
    """按传入 SKUs 获取商品数据及统计信息，便于上层分析缺失情况。"""
    api = DSZProductsAPI()

    # todo 构建自己使用的sku结构题，只保留需要的字段？
    return api.fetch_by_skus(skus, return_stats=True)  # type: ignore[return-value]



"""
 对外暴露：按 SKU 批量获取 zone rates（仅 sku + standard）
    调用 /v2/get_zone_rates，按传入 skus（逗号分隔）获取每个 sku 的 'standard' 区域费率。
    只返回 [{'sku': 'ABC', 'standard': {...}}, ...]
"""
def get_zone_rates_by_skus(skus: Iterable[str]) -> List[dict]:
    api = DSZProductsAPI()
    attempts = 2
    backoff = 1.0

    last_err: Optional[Exception] = None
    for i in range(1, attempts + 1):
        try:
            return api.fetch_zone_rates_by_skus(skus)
        except Exception as e:  # noqa: BLE001
            last_err = e
            if i >= attempts:
                logger.error(
                    "get_zone_rates_by_skus failed after %d attempts; err=%s",
                    i, e
                )
                raise
            # 进入重试分支，打 info 便于观测
            logger.info(
                "get_zone_rates_by_skus attempt %d/%d failed: %s; retrying in %.1fs",
                i, attempts, e, backoff
            )
            time.sleep(backoff)
            # 如需指数退避可改为：backoff *= 2

    # 理论上不会走到这里，兜底抛出
    if last_err:
        raise last_err
    return []



class DSZProductsAPI:
    """封装 DSZ /v2/products 查询，处理批次拆分、重试和结果统计。"""

    # 注入创建 DSZHttpClient, 写配置
    def __init__(self, http: Optional[DSZHttpClient] = None) -> None:
        """允许注入自定义 DSZHttpClient，便于测试或多账号使用。"""
        self.http = http or DSZHttpClient()
        self.endpoint = settings.DSZ_PRODUCTS_ENDPOINT      # 默认：/v2/products
        self.max_per_req = settings.DSZ_PRODUCTS_MAX_PER_REQ # 默认：50
        self.sku_param = settings.DSZ_PRODUCTS_SKU_PARAM    # 默认：skus
        self.payload_sku_field = settings.DSZ_PRODUCTS_SKU_FIELD  # 默认：sku    
        self.method = (settings.DSZ_PRODUCTS_METHOD or "GET").strip().upper()

        # zone rates 端点与限制
        self.zone_endpoint = settings.DSZ_ZONE_RATES_ENDPOINT           # /v2/get_zone_rates
        self.zone_limit = int(getattr(settings, "DSZ_ZONE_RATES_LIMIT", 160))  # 硬上限 160
        self.zone_method = (getattr(settings, "DSZ_ZONE_RATES_METHOD", "POST") or "POST").upper()
    

    # test ✅
    """批量查询 SKUs，支持统计/重试策略，自动去重并记录缺失/多余。"""
    def fetch_by_skus(
        self,
        skus: Iterable[str],
        *,
        return_stats: bool = False,
        on_error: str = "skip",                 # 'skip' or 'raise'
        per_batch_attempts: int = 2,            # 子批层再尝试次数（叠加 http_client 内部重试3次）
        per_batch_backoff_sec: float = 0.5,     # 子批层尝试间隔
        collect_failed_detail: bool = True,     # 默认收集失败/缺失/多余明细
    ) -> Tuple[List[dict], Dict[str, Any]] | List[dict]:

        all_skus = [s.strip() for s in skus if s and s.strip()]
        if not all_skus:
            return ([], _empty_stats()) if return_stats else []

        results: List[dict] = []
        seen: set[str] = set()
        stats = _empty_stats()
        stats["requested_total"] = len(all_skus)

        # 每批“实际请求大小” = min(DSZ_BATCH_SIZE, 接口硬上限)
        per_req = self.max_per_req
        # 目前default的limit是40 ，可以通过添加&limit=50 来达到50个每页

        # 逐批调用
        for chunk in _chunked(all_skus, per_req):
            items = self._fetch_chunk_items(
                chunk,
                per_batch_attempts=per_batch_attempts,
                per_batch_backoff_sec=per_batch_backoff_sec,
                on_error=on_error,
                collect_failed_detail=collect_failed_detail,
                stats=stats,
            )
            self._process_chunk_results(
                chunk=chunk,
                items=items,
                results=results,
                seen=seen,
                stats=stats,
                collect_failed_detail=collect_failed_detail,
            )

        stats["returned_total"] = len(results)
        return (results, stats) if return_stats else results



    # -------- 单次DSZ接口调用 --------
    # test ✅
    def _fetch_one_batch(self, skus: List[str]) -> Any:
        """调用一次 DSZ /v2/products，处理单批最多 max_per_req 个 SKU。"""
        params: Dict[str, Any] = {
            self.sku_param: ",".join(skus),
            "limit": min(self.max_per_req, max(len(skus), 1)),
        }
        return self.http.get_json(self.endpoint, params=params)


    def _fetch_chunk_items(
        self,
        chunk: List[str],
        *,
        per_batch_attempts: int,
        per_batch_backoff_sec: float,
        on_error: str,
        collect_failed_detail: bool,
        stats: Dict[str, Any],
    ) -> List[dict]:
        attempt = 0
        while True:
            attempt += 1
            try:
                payload = self._fetch_one_batch(chunk)
                return self._extract_items(payload)
            except Exception as e:
                if attempt >= per_batch_attempts:
                    if on_error == "raise":
                        raise
                    logger.error(
                        "DSZ sub-batch failed after %d attempts; skip. size=%d; sample=%s; err=%s",
                        attempt, len(chunk), chunk[:5], e
                    )
                    self._record_failed_batch(chunk, stats, collect_failed_detail)
                    return []
                time.sleep(per_batch_backoff_sec)


    def _process_chunk_results(
        self,
        *,
        chunk: List[str],
        items: List[dict],
        results: List[dict],
        seen: set[str],
        stats: Dict[str, Any],
        collect_failed_detail: bool,
    ) -> None:
        returned: set[str] = set()

        self._merge_items(
            items,
            results=results,
            seen=seen,
            returned=returned,
        )

        req_set = set(chunk)
        missing = req_set - returned
        extra = returned - req_set

        if missing:
            retry_items = self._retry_missing_skus(list(missing))
            if retry_items:
                logger.info(
                    "DSZ products retry missing skus: requested=%d missing_before=%d retry_count=%d sample=%s",
                    len(req_set),
                    len(missing),
                    len(retry_items),
                    list(sorted(missing))[:5],
                )
                self._merge_items(
                    retry_items,
                    results=results,
                    seen=seen,
                    returned=returned,
                )
                missing = req_set - returned
                extra = returned - req_set

        if missing or extra:
            logger.warning(
                "DSZ products mismatch: requested=%d, returned=%d, missing=%d, extra=%d; sample_missing=%s; sample_extra=%s",
                len(req_set), len(returned), len(missing), len(extra),
                list(sorted(missing))[:5], list(sorted(extra))[:5]
            )

        self._record_missing_extra(
            missing,
            extra,
            stats=stats,
            collect_failed_detail=collect_failed_detail,
        )


    def _merge_items(
        self,
        items: List[dict],
        *,
        results: List[dict],
        seen: set[str],
        returned: set[str],
    ) -> None:
        for it in items:
            sku = self._extract_sku(it)
            if sku:
                returned.add(sku)
            if (not sku) or (sku not in seen):
                results.append(it)
                if sku:
                    seen.add(sku)


    def _record_failed_batch(
        self,
        chunk: List[str],
        stats: Dict[str, Any],
        collect_failed_detail: bool,
    ) -> None:
        stats["failed_batches_count"] += 1
        stats["failed_skus_count"] += len(chunk)
        if not collect_failed_detail:
            return
        stats["failed_sku_list"].extend(chunk[: self.max_per_req])
        sample = stats["failed_skus_sample"]
        need = max(0, 20 - len(sample))
        if need:
            sample.extend(chunk[:need])

    def _record_missing_extra(
        self,
        missing: set[str],
        extra: set[str],
        *,
        stats: Dict[str, Any],
        collect_failed_detail: bool,
    ) -> None:
        stats["missing_count"] += len(missing)
        stats["extra_count"] += len(extra)
        if not collect_failed_detail:
            return
        if missing:
            stats["missing_sku_list"].extend(list(missing)[: self.max_per_req])
        if extra:
            stats["extra_sku_list"].extend(list(extra)[: self.max_per_req])


    def _retry_missing_skus(self, missing_skus: List[str]) -> List[dict]:
        """落地一次补偿请求：仅针对缺失 SKU，再拉取一次。失败时静默回退。"""
        if not missing_skus:
            return []
        try:
            payload = self._fetch_one_batch(missing_skus)
            return self._extract_items(payload)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Retry missing SKUs failed: count=%d sample=%s err=%s",
                len(missing_skus),
                missing_skus[:5],
                exc,
            )
            return []
    


            
    # test ✅
    # 从复杂 payload 中提取商品列表，并确保最终是 list[dict]，不会改动每个商品的内容
    # 负责把 DSZ 接口返回的原始 JSON 中真正的商品列表提取出来，并确保最终拿到的是 list[dict]
    # 优先支持 DSZ 的 { "result": [...] } 结构；否则回退到常见键并递归查找
    def _extract_items(self, payload: Any) -> List[dict]:

        # recursion
        def _ensure_dict_list(value: Iterable[Any], label: str) -> List[dict]:
            if not isinstance(value, list):
                raise DSZPayloadError(f"{label} is not a list")
            if not value:
                return []
            if all(isinstance(x, dict) for x in value):
                return list(value)
            raise DSZPayloadError(f"{label} contains non-dict item")

        if isinstance(payload, list):
            return _ensure_dict_list(payload, "products payload list")
        
        # step 1 - 先优先检查 result / results / products / items / data / payload / response 这些常见字段；
        # 发现是 list 就校验并返回，发现还是 dict 则递归继续找
        if isinstance(payload, dict):
            preferred_keys = (
                "result",
                "results",
                "products",
                "items",
                "data",
                "payload",
                "response",
            )

            for key in preferred_keys:
                if key not in payload:
                    continue
                value = payload[key]
                if isinstance(value, list):
                    return _ensure_dict_list(value, f"{key} list")
                if isinstance(value, dict):
                    try:
                        return self._extract_items(value)
                    except DSZPayloadError:
                        continue


            # step 2 - 如果优先字段都没命中，就广度优先遍历所有嵌套字典的键值，寻找第一个 list[dict]，找到后立即返回
            queue: List[dict] = [payload]
            idx = 0
            while idx < len(queue):
                current = queue[idx]
                idx += 1
                for key, value in current.items():
                    if isinstance(value, list):
                        try:
                            return _ensure_dict_list(value, f"{key} list")
                        except DSZPayloadError:
                            continue
                    if isinstance(value, dict):
                        queue.append(value)

        raise DSZPayloadError(
            f"unexpected products payload structure: {type(payload)}; "
            f"keys={list(payload.keys()) if isinstance(payload, dict) else 'N/A'}"
        )



    """
        提取返回项里的 sku，用于去重/一致性检查。
        优先使用 DSZ_PRODUCTS_SKU_FIELD；再尝试常见备选键名。
        读取商品项中的 SKU 字段，缺失时返回 None。
        只拿出 SKU 用来去重与统计。
    """
    def _extract_sku(self, item: dict) -> Optional[str]:
        # 只看官方字段 sku
        if not isinstance(item, dict):
            return None
        v = item.get(self.payload_sku_field)
        return v if isinstance(v, str) and v else None    
    


    # ----------------- /v2/get_zone_rates -----------------
    """
        调用 /v2/get_zone_rates。
        输入：任意 SKU 列表（自动按 160/批拆分）。
        输出：合并后的 list[{'sku': 'ABC', 'standard': {...}}]
    """
    def fetch_zone_rates_by_skus(self, skus: Iterable[str]) -> List[dict]:
        
        all_skus = [s.strip() for s in skus if s and s.strip()]
        if not all_skus:
            return []
        
        # 每批“实际请求大小” = min(DSZ_BATCH_SIZE, 接口硬上限)
        per_req = settings.DSZ_ZONE_RATES_LIMIT

        results: List[dict] = []
        seen: set[str] = set()

        for chunk in _chunked(all_skus, per_req):
            body = {"skus": ",".join(chunk), "page_no": 1, "limit": per_req}

            # 打印请求入参，便于调试（优先美化 JSON，回退到原对象）
            # try:
            #     print("DSZ zone_rates request body:\n" + json.dumps(body, ensure_ascii=False, indent=2, default=str))
            # except Exception:
            #     print("DSZ zone_rates request body (fallback):", body)

            payload = self.http.post_json(self.zone_endpoint, json_body=body) 

            # 解析 items
            items = self._extract_zone_rates_items(payload)
            # print(f"DSZ zone_rates items size={len(items)}")

            returned = {str((it or {}).get("sku") or "").strip() for it in items if isinstance(it, dict)}
            returned.discard("")
            if len(returned) < len(chunk):
                missing = [sku for sku in chunk if sku not in returned]
                logger.warning(
                    "DSZ zone_rates mismatch: requested=%d returned=%d missing=%d sample_missing=%s",
                    len(chunk), len(returned), len(missing), missing[:5],
                )

            for it in items:
                sku = (it.get("sku") or "").strip()
                if not sku:
                    continue
                # 只要 sku + standard
                obj = {"sku": sku, "standard": it.get("standard")}
                if sku not in seen:
                    results.append(obj)
                    seen.add(sku)

        return results
    

    
    def _extract_zone_rates_items(self, payload: Any) -> List[dict]:
        """
        严格按文档结构解析：
        {
          "result": [ { "sku": "...", "standard": {...}, ... }, ... ],
          "code": 1,
          ...
        }
        """
        if not isinstance(payload, dict):
            raise DSZPayloadError("zone_rates payload is not a dict")

        result = payload.get("result")
        if not isinstance(result, list):
            raise DSZPayloadError("zone_rates.result is not a list")

        out: List[dict] = []
        for x in result:
            if not isinstance(x, dict):
                continue
            sku = x.get("sku")
            if not isinstance(sku, str) or not sku.strip():
                continue
            # 保留 whole item，之后取 .get('standard')
            out.append(x)
        return out






# 将任意长度的 SKU 列表按照 size 切分为若干子批。自动跳过空/纯空白字符串
def _chunked(seq: List[str], size: int) -> Iterable[List[str]]:
    """把 SKU 列表切成 size 大小的子列表，自动跳过空值。"""
    buf: List[str] = []
    for s in seq:
        if not s:
            continue
        ss = s.strip()
        if not ss:
            continue
        buf.append(ss)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


'''
产出统计骨架：请求量、返回量、缺失/多余数量、失败子批/失败 SKU 计数与采样等
'''
def _empty_stats() -> Dict[str, Any]:
    """生成统计结构的初始值，便于累积各项计数。"""
    return {
        "requested_total": 0,
        "returned_total": 0,
        "missing_count": 0,
        "extra_count": 0,
        "failed_batches_count": 0,
        "failed_skus_count": 0,
        "failed_skus_sample": [],
        # 便于补偿/运维
        "failed_sku_list": [], 
        "missing_sku_list": [],
        "extra_sku_list": [],        
    }
