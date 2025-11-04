# """Integration-focused debug tests for DSZHttpClient."""

# from __future__ import annotations

# from datetime import datetime, timezone
# import time
# from typing import Iterable

# import pytest

# from app.core.config import settings
# from app.integrations.dsz.http_client import DSZHttpClient


# def _has_dsz_credentials() -> bool:
#     return bool(settings.DSZ_API_EMAIL and settings.DSZ_API_PASSWORD)


# pytestmark = [
#     pytest.mark.integration,
#     pytest.mark.skipif(
#         not _has_dsz_credentials(),
#         reason="DSZ credentials (DSZ_API_EMAIL / DSZ_API_PASSWORD) not configured.",
#     ),
# ]



# @pytest.fixture(scope="function")
# def dsz_http_client() -> Iterable[DSZHttpClient]:
#     dsz_http_client = DSZHttpClient()
#     try:
#         yield dsz_http_client
#     finally:
#         dsz_http_client._session.close()   # 资源释放



# # 独立 只测鉴权拿 token： 验证 /auth 能成功获取 token，并且过期时间在未来。
# # 只测鉴权流程：调用 _authenticate(force=True) 后，_token 存在、value 非空、expires_at 晚于当前时间。
# # 用于单步调试 /auth 交换 token 的细节。
# def test_authenticate_fetches_token(dsz_http_client: DSZHttpClient) -> None:
#     dsz_http_client._authenticate(force=True)
#     token = getattr(dsz_http_client, "_token", None)
#     assert token is not None, "Token should be populated after authenticate"
#     assert token.value and token.value.lower() != "none", "Token value looks invalid"
#     assert token.expires_at > datetime.now(timezone.utc), "Token expiry is not in the future"



# # 单独验证 _last_request_ts 是否在一次成功调用后更新
# # 只验证：一次调用 /v2/products 后 _last_request_ts 被更新
# # 只测时间戳更新：请求一次 /v2/products 后，私有字段 _last_request_ts 是否被更新，且不比调用前更小。
# # 便于观察 pacing/节流的实际生效点。
# def test_last_request_ts_updates_after_call(dsz_http_client: DSZHttpClient) -> None:

#     skus = _sample_skus()
#     if not skus:
#         pytest.skip("Provide TEST_DSZ_SKU or TEST_DSZ_SKUS to run this test.")
#     params = {settings.DSZ_PRODUCTS_SKU_PARAM: ",".join(skus[:1])}

#     before = getattr(dsz_http_client, "_last_request_ts", 0.0)  # 可能为 0.0
#     dsz_http_client.get_json(settings.DSZ_PRODUCTS_ENDPOINT, params=params)
#     after = getattr(dsz_http_client, "_last_request_ts", 0.0)

#     assert after > 0.0, "_last_request_ts should be set after a successful request"
#     # before 可能是 0 或上一次测试遗留（function-scope 已最大限度隔离）
#     assert after >= before, "_last_request_ts should be monotonic non-decreasing"



# # 只负责断言“返回类型是 list 或 dict”，不再兼顾非空性
# # 只测返回类型：对一组 SKU 请求后，响应应该是 list 或 dict（两者之一）。不关心内容是否为空
# def test_products_returns_list_or_dict():

#     dsz_http_client = DSZHttpClient()  # 每次独立实例

#     skus = _sample_skus()
#     if not skus:
#         pytest.skip("Provide TEST_DSZ_SKU or TEST_DSZ_SKUS to verify /v2/products payload type.")

#     params = {settings.DSZ_PRODUCTS_SKU_PARAM: ",".join(skus)}
#     payload = dsz_http_client.get_json(settings.DSZ_PRODUCTS_ENDPOINT, params=params)

#     dsz_http_client._session.close()  # type: ignore[attr-defined]

#     assert isinstance(payload, (list, dict)), f"Unexpected payload type: {type(payload)}"



# # 只负责断言“在提供有效 SKU 时，返回非空”
# # 只测非空性：当传入至少一个真实存在的 SKU 时，返回体不应为空。用于排查“查不到数据/参数拼错”这类问题
# def test_products_nonempty_for_valid_sku():

#     dsz_http_client = DSZHttpClient()  # 每次独立实例

#     skus = _sample_skus()
#     if not skus:
#         pytest.skip("Provide TEST_DSZ_SKU or TEST_DSZ_SKUS to verify non-empty payload.")

#     params = {settings.DSZ_PRODUCTS_SKU_PARAM: ",".join(skus[:1])}
#     payload = dsz_http_client.get_json(settings.DSZ_PRODUCTS_ENDPOINT, params=params)

#     dsz_http_client._session.close()  # type: ignore[attr-defined]

#     if isinstance(payload, list):
#         assert payload, "Empty list returned; confirm the test SKU exists."
#     elif isinstance(payload, dict):
#         assert payload, "Empty dict returned; confirm the test SKU exists."
#     else:
#         pytest.fail(f"Unexpected payload type: {type(payload)}")



# # 验证两次调用的“时间顺序”（不严格要求间隔阈值）
# # 只测调用顺序：两次连续调用后，第二次的 _last_request_ts 不早于第一次。我们不要求严格间隔时间，
# # 避免 CI/网络抖动引发误报，主要用于确认“本地节流记录点”
# def test_rate_limit_respected_between_calls():

#     dsz_http_client = DSZHttpClient()  # 每次独立实例

#     skus = _sample_skus()
#     if not skus:
#         pytest.skip("Provide TEST_DSZ_SKU or TEST_DSZ_SKUS to verify pacing behaviour.")
#     params = {settings.DSZ_PRODUCTS_SKU_PARAM: ",".join(skus[:1])}

#     dsz_http_client.get_json(settings.DSZ_PRODUCTS_ENDPOINT, params=params)
#     first_sent = getattr(dsz_http_client, "_last_request_ts", 0.0)

#     dsz_http_client.get_json(settings.DSZ_PRODUCTS_ENDPOINT, params=params)
#     second_sent = getattr(dsz_http_client, "_last_request_ts", 0.0)

#     dsz_http_client._session.close()  # type: ignore[attr-defined]

#     assert first_sent > 0.0, "First call should have updated _last_request_ts"
#     assert second_sent >= first_sent, "Second call should not be recorded before first call"



# # 黑盒验证连续两次调用都能成功（不依赖内部私有字段）
# def test_two_back_to_back_calls_succeed(dsz_http_client: DSZHttpClient):
#     """验证：连续两次调用同一路径都能成功返回（不看内部节流字段，仅黑盒验证）。"""
#     skus = _sample_skus()
#     if not skus:
#         pytest.skip("Provide TEST_DSZ_SKU or TEST_DSZ_SKUS to run this test.")
#     params = {settings.DSZ_PRODUCTS_SKU_PARAM: ",".join(skus[:1])}

#     payload1 = dsz_http_client.get_json(settings.DSZ_PRODUCTS_ENDPOINT, params=params)
#     payload2 = dsz_http_client.get_json(settings.DSZ_PRODUCTS_ENDPOINT, params=params)

#     # 只断言都成功解析为 JSON
#     assert isinstance(payload1, (list, dict))
#     assert isinstance(payload2, (list, dict))




# # 单独测试会话能否安全关闭（资源释放）
# def test_session_can_close_safely():
#     """只验证：Session 关闭不抛异常。"""
#     client = DSZHttpClient()
#     client._session.close()  # type: ignore[attr-defined]



# # 功能通了之后，测试：
# # 1、分批
# # 2、异常
# # 3、一分钟超过60次

# def test_get_products_by_skus_respects_rate_limit(monkeypatch: pytest.MonkeyPatch) -> None:
#     """调用 get_products_by_skus 两次，确认底层节流间隔满足配置。"""

#     from app.integrations.dsz import dsz_products

#     skus = _sample_skus()
#     if not skus:
#         pytest.skip("Provide TEST_DSZ_SKU or TEST_DSZ_SKUS to run this test.")

#     http_client = DSZHttpClient()
#     def factory(*args, **kwargs):
#         kwargs.setdefault("http", http_client)
#         return dsz_products.DSZProductsAPI(*args, **kwargs)

#     monkeypatch.setattr(dsz_products, "DSZProductsAPI", factory)

#     try:

#         start = time.perf_counter()
#         result1 = dsz_products.get_products_by_skus_with_stats(skus)
#         duration_first = time.perf_counter() - start
#         print(f"get_products_by_skus_with_stats took {duration_first:.3f}s")
#         first_sent = http_client._last_request_ts  # type: ignore[attr-defined]

#         start = time.perf_counter()
#         result2 = dsz_products.get_zone_rates_by_skus(skus)
#         duration_second = time.perf_counter() - start
#         print(f"get_zone_rates_by_skus took {duration_second:.3f}s")
#         second_sent = http_client._last_request_ts  # type: ignore[attr-defined]
#     finally:
#         http_client._session.close()  # type: ignore[attr-defined]

#     assert isinstance(result1, list), "Expected list of products on first call"
#     assert isinstance(result2, list), "Expected list of products on second call"

#     interval = 60.0 / float(http_client.rate_limit_per_min)
#     assert second_sent >= first_sent, "Second call timestamp should not precede first call"
#     delta = second_sent - first_sent
#     assert delta >= interval - 0.15, "Calls should be spaced by the configured rate limit interval"


# def test_get_zone_rates_by_skus_respects_rate_limit() -> None:
#     """调用 get_zone_rates_by_skus 两次，确认真实流程下的节流间隔。"""

#     from app.integrations.dsz import dsz_products

#     skus = _sample_skus()
#     if not skus:
#         pytest.skip("Provide TEST_DSZ_SKU or TEST_DSZ_SKUS to run this test.")


#     start = time.perf_counter()
#     result1 = dsz_products.get_products_by_skus_with_stats(skus)
#     duration_first = time.perf_counter() - start
#     print(f"fetch_zone_rates_by_skus call #1 took {duration_first:.3f}s")

#     start = time.perf_counter()
#     result2 = dsz_products.get_zone_rates_by_skus(skus)
#     duration_second = time.perf_counter() - start
#     print(f"fetch_zone_rates_by_skus call #2 took {duration_second:.3f}s")
    
#     assert isinstance(result1, list), "Zone rate response should be a list"
#     assert isinstance(result2, list), "Zone rate response should be a list"



# # 测试sku 
# def _sample_skus() -> list[str]:
#     # 87个
#     return [
#         "FPIT-BBQ-X-BRIDGE",
#         "BBQ-AGRILL-ST-REBK",
#         "SAND-JUMBO-CANOPY",
#         "V274-AQ-SP3000",
#         "V240-T0090",
#         "V178-36024",
#         "BBQ-SMOKER-3IN1",
#         "BBQ-SMOKER-CRM-RECT",
#         "WS-H2950-WIFI-MF-UVL",
#         "ODF-KID-PICNIC-UM-CFL",
#         "CASE-MU-HZ7002-DIPI",
#         "GARDEN-ALUMGR-21-FC2",
#         "MM-STAND-2530LED-BK",
#         "OSB-S190-BK",
#         "TW-C-S-SS",
#         "CASE-MU-002-DIBK",
#         "PET-STROLLER-4WL-BK",
#         "MM-STAND-FRAME-WH",
#         "PET-STROLLER-4WL-M-BK",
#         "PET-RAB-CAGE-H97",
#         "HM-BED-TASSEL-COT-CR",
#         "PLAY-MAKEUP-30",
#         "SH-CL-366X100-100-BE",
#         "RL-FL-006-BK-LV",
#         "ODF-KID-PICNIC-UM-NW",
#         "ODF-CHAIR-TEA-BK",
#         "PET-CAT-402-GR",
#         "LA-DESK-LEO-L1-WH",
#         "FLOOR-CAMP-0156R-BK",
#         "BA-TW-9078-BKX2",
#         "PIC-BAS-4P-GRBU",
#         "PICNIC-4PPL-STRIPE-BUPK",
#         "PICNIC-4PPL-BASKET-BK",
#         "PICNIC-2PPL-BASKET-WH",
#         "MET-DESK-105-BK",
#         "PICNIC-4PPL-CHEESE-BR",
#         "LA-DESK-60-LW",
#         "PICNIC-4PPL-COOLER-NAVY",
#         "LA-DESK-60-WH",
#         "V97-7MCARIBE",
#         "PUMP-GARDEN-1500",
#         "PUMP-JET-2300",
#         "PUMP-TPC-11-YEL",
#         "BIN-WALL-44",
#         "PUMP-GARDEN-800",
#         "PUMP-SUBM-1800",
#         "PUMP-MAC-600",
#         "BIN-WALL-48",
#         "PUMP-TPC-21-BK",
#         "PUMP-QB60",
#         "V324-21SM",
#         "V210-2456740",
#         "V177-D-BW220",
#         "V178-83089",
#         "V177-D-BR1070",
#         "V177-D-WBBR253",
#         "V200-TK5284M",
#         "V200-TK5274M",
#         "MOWER-LI37-N-20VX2",
#         "V177-D-WBBR251",
#         "AC-320L-RD",
#         "SANDER-DHAND-710",
#         "PCAW-20V-SAWTM",
#         "AC-200L-GD",
#         "CSAW-20V-10IN-RD",
#         "ST-2000W-BL",
#         "AC-85L-SL",
#         "ST-1000W-BL",
#         "PCAW-20V-SAW",
#         "GREASE-20V-10000-RD",
#         "V240-TRP-PAD-BU-06",
#         "V210-2669087",
#         "V240-CS-500KG",
#         "V413-CM-720",
#         "V240-TRA-FJP-45-YLPU",
#         "V413-RT-100",
#         "V177-BPEL10M",
#         "V177-D-BPR4090B6P",
#         "V240-TRP-PAD-BU-14",
#         "V177-D-LX203M",
#         "V177-AL-MBMP05780",
#         "V28-ELEDIGEIIR802H",
#         "V177-60-XE6360C",
#         "V177-D-AV980054",
#         "V200-FCCOM1259000",
#         "V177-AL-MBMP01978",
#         "V177-AL-MEA070150",
#     ]
