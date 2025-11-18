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

#     skus = _sample_skus1()
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



# def _sample_skus1() -> list[str]:
#     return ["V1026-KLCHH24100WH"]



# # 测试sku 
# def _sample_skus() -> list[str]:
#     # 300个
#     return [
#         "FPIT-BBQ-X-BRIDGE", "BBQ-AGRILL-ST-REBK", "SAND-JUMBO-CANOPY", 
#          "V274-AQ-SP3000", "V240-T0090", "V178-36024", "BBQ-SMOKER-3IN1", "BBQ-SMOKER-CRM-RECT", 
#          "WS-H2950-WIFI-MF-UVL", "ODF-KID-PICNIC-UM-CFL", "CASE-MU-HZ7002-DIPI", "GARDEN-ALUMGR-21-FC2", 
#          "MM-STAND-2530LED-BK", "OSB-S190-BK", "TW-C-S-SS", "CASE-MU-002-DIBK", "PET-STROLLER-4WL-BK", "MM-STAND-FRAME-WH", "PET-STROLLER-4WL-M-BK", "PET-RAB-CAGE-H97", "HM-BED-TASSEL-COT-CR", "PLAY-MAKEUP-30", "SH-CL-366X100-100-BE", "RL-FL-006-BK-LV", 
#          "ODF-KID-PICNIC-UM-NW", "ODF-CHAIR-TEA-BK", "PET-CAT-402-GR", "LA-DESK-LEO-L1-WH", "FLOOR-CAMP-0156R-BK", "BA-TW-9078-BKX2", "PIC-BAS-4P-GRBU", "PICNIC-4PPL-STRIPE-BUPK", "PICNIC-4PPL-BASKET-BK", "PICNIC-2PPL-BASKET-WH", "MET-DESK-105-BK", 
#          "PICNIC-4PPL-CHEESE-BR", "LA-DESK-60-LW", "PICNIC-4PPL-COOLER-NAVY", "LA-DESK-60-WH", "V97-7MCARIBE", "PUMP-GARDEN-1500", 
#          "PUMP-JET-2300", "PUMP-TPC-11-YEL", "BIN-WALL-44", "PUMP-GARDEN-800", "PUMP-SUBM-1800", "PUMP-MAC-600", "BIN-WALL-48", "PUMP-TPC-21-BK", "PUMP-QB60", "V324-21SM", "V210-2456740", "V177-D-BW220", "V178-83089", "V177-D-BR1070", "V177-D-WBBR253", 
#          "V200-TK5284M", "V200-TK5274M", "MOWER-LI37-N-20VX2", "V177-D-WBBR251", "AC-320L-RD", "SANDER-DHAND-710", "PCAW-20V-SAWTM", "AC-200L-GD", "CSAW-20V-10IN-RD", "ST-2000W-BL", "AC-85L-SL", "ST-1000W-BL", "PCAW-20V-SAW", "GREASE-20V-10000-RD", "V240-TRP-PAD-BU-06", 
#          "V210-2669087", "V240-CS-500KG", "V413-CM-720", "V240-TRA-FJP-45-YLPU", "V413-RT-100", "V177-BPEL10M",
#            "V177-D-BPR4090B6P", "V240-TRP-PAD-BU-14", "V177-D-LX203M", "V177-AL-MBMP05780", 
#          "V28-ELEDIGEIIR802H", "V177-60-XE6360C", "V177-D-AV980054", "V200-FCCOM1259000", "V177-AL-MBMP01978", "V177-AL-MEA070150", "V177-D-AV959413", "V177-AL-MBMP05775", "V177-L-SPMB-MB-TWS-E2", "FIK-TAPE-2000M", "V177-D-BPR1060B6P", "V177-AL-MBMP02901", "ST-500W-BL", "FIK-TAPE-1200M", "V177-D-BPR1438R6P",
#          "V177-D-BPT750W", "V177-D-BPTP300BT", "V177-D-LX25A0013", "V177-D-DY31000", "DIFF-307-LW", 
#          "GWH-PUMP-43-BK", "MATTRESS-TOP-7Z-5-BL-Q", "BAM-SHED-PILLOWX2", "CC-WL-IP-FC2", "PILLOW-LT-SHED-X2", "DIFF-519W-LW", "VAC-CL-BH-BK", "MATTRESS-TOP-8-D", "CCTV-8C-4D-BK", "MA-D-GAS-BK", "FD-B-1159-TRAY-X2", "CCTV-8C-8B-BK-T", "PP-10-75X300-BK", "DIFF-G3-DW", 
#          "CCTV-4C-2B-BK-T", "MA-B-D-C24-BK", "DIFF-166-LW", "CTH-2400-BK", "CWH-2000-BK", "CCT-B-2B-3Z-TOUCH-BK", "VAC-STD-SY02-BK", "VAC-008-RD", "VAC-CD-AH-RD-AL", "AF-K-G-5T6-SS", "VAC-008-BL", "VAC-008-BK", "CDF-D4C-SINGLE", "VAC-CD-AH-PP-AL", "VAC-CL-150-FT", "IM-ZB-12B-RD",
#          "FD-B-1159-SS", "AFB-12T-20", "FD-B-1159-SS-7", "CCTV-4C-4D-BK", "AFB-CORNER-20", "WD-BP-F22B", "AFB-12T-60", "CCTV-4C-4B-BK", "AFB-12T-40", "TV-MOUN-B-SINGLE-66MT", "PUMP-POOL-MAX1200", "SPK-WALL-MUB1035", "BW-FIL-PUMP-58381", "PP-10-75X600-BK", "CCTV-4C-4S-BK", "TV-MOUN-B-DOUB-08MT", "TV-MOUN-WALL-117-BK", 
#          "TV-MOUN-B-DOUB-36BT", "SPK-CEILING-MSR127", "WTINT-100CM-VLT15-5C", "CCTV-8C-4B-BK", "CCTV-IP-BLACK-FC2", 
#          "TP-WT6663A", "CCTV-4C-2B-BK", "TP-WT3750BK", "CCTV-IP-BLACK", "TP-WT3750", "WTINT-100CM-VLT35-5C", "TP-WT3520P", "V240-TSR-4SL-GST-BK", "V28-ELECHOH049", "FD-B-1149-10T-BK", "DH-002-BK-WH", "V240-FS-190-2A", "V178-62984", "DIFF-X011-LW", "V240-TSR-4SL-GST-WH", "V240-FS-190-2A-BK", "V178-60784", "FURNI-G-TOY111-WH", "LC-FURNI-COF02-WD", "CT-GAS-2B-BK", "V177-D-DY32500", "PLAY-MARKET-TROLLEY", "V28-ACBUGN40277", "CT-GAS-2B-SS", "PLAY-CAR-BULLDOZER", "V188-ZAP-TLL2411-15-PINK-L", "PLAY-CASTLE-BU", 
#          "RCAR-S1000RR-BK", "V97-4MCARIBE", "SAND-SQUARE-95", "PLAY-UMBRELLA-BU", "ST-CAB-1D-1B-WHX2", "RCAR-S1000RR-RD", "BW-POOL-PLAY-53068", "RCAR-S1000RR-BU", "SAND-CANOPY-110", "SAND-CANOPY-WATER", "INVERT-P-600W-SL", "INVERT-P-1000W-SL", "WTINT-76CM-VLT15-5C", "INVERT-P-1500W-SL", "WTINT-76CM-VLT5", "ST-200W-BL", 
#          "WTINT-76CM-VLT15", "WTINT-76CM-VLT35", "WTINT-100CM-VLT5-5C", "WTINT-TOOL-5C", "TAN-FIX-700-BLACK", "AR-GRASS-TAPE-20M", "AR-GRASS-PINS-100", "AR-GRASS-TAPE-10M", "CASE-HZ8-040-CROBK", "CASE-MU-HZ7002-BK", "AR-GRASS-PINS-200", "CASE-MU-002-GDBK", "TAN-VEN-BOX-BLACK", "DIY-CR-605-BK", "PILLOW-WEDGE-BEI", "PILLOW-LT-CONTOUR-X2", "QCS-DIAM-BK-Q", "PILLOW-WEDGE-BU", "EB-POLY-MC-Q", "EB-POLY-MC-K", "QCS-DIAM-BK-K", "EB-POLY-MC-S", "MATTRESS-CON-PILLOW-GELX2", "EB-POLY-MC-D", "PFS-26F-WH", "TAP-A-82H37-SI", 
#          "CB-005-WH", "GA-SDOOR-BK", "GA-SDOOR-D-183", "SINK-3045-R010", "SINK-3945-R0-SI", "POT-BIN-20L-SET", "PFS-26F-BK", "PFS-11F-BK", "SPRAYER-PART-BOOM", "GA-SDOOR-D-244", "GO-SENSOR-LM102", "BW-BED-Q-56-67614", "TAP-A-81H14-SI", "TAP-A-81H36-SI", "TAP-A-82H35-BK", "SINK-BLACK-3045", "TAP-A-81H14-BK", "TAP-A-82H35-SI", "SSKB-WHEEL-4", "SHOEBOX-PP-20", "FURNI-WALL-SHELF-BK", "POT-BIN-15L-SET", "CASE-CD-500-SL", "POT-BIN-20L-SET-WH", "FURNI-WALL-SHELF-WH", "POT-BIN-15L-SET-WH", "GL-NM-3T-1200", 
#          "BW-POOL-PLAY-53052", "COVER-CV-DCS-M", "TARP-BKSI-3X36", "TARP-BKSI-36X48", "TARP-BKSI-3X45", 
#          "TARP-BKSI-36X6", "BW-FL-BED-S-22", "COVER-CV-DCS-S", "CAMP-MAT-INF-DF-GREY", "TARP-BKSI-36X73", 
#          "COVER-CV-DCS-XS", "AWN-CV-B-SS-46", "BIKE-4-BK", "MDETECTOR-C-GC1066", "MDETECTOR-C-GC1037", "MDETECTOR-C-GC1028", "MDETECTOR-C-GC1010", "BIKE-3-BK", "MDETECTOR-C-GC1065", "AWN-CV-B-SS-40", "MDETECTOR-C-GC2007", "LAMP-FLOOR-SF-3017-A-BK", "AQUA-CFP-10000", "ATM-3-1-01M-MG-AP1", "FIK-ROPE-500M", "ATM-3-1-01M-PK-AP1", "ATM-3-1-01M-MG", "ATM-3-1-01M-PK", "ATM-4-1-01M-MG", "CSAW-SEF-22-RDWH", "ATM-BK-AP", "SALON-B-4128-NEW-BK", "SALON-B-4128-BK", "TENT-C-BEA-4P", "FIK-TAPE-LOCK-400M", 
#          "TENT-C-CA6", "VP-430-WIFI-BT-WH", "SALON-B-RD-BK", "FIK-WIRE-LOCK-500M", "CC-QUILTSET-S-RD-BG", "SALON-B-RD-WH", "XMAS-LED-800-IC-WW", "DO-PUMP-DIGIT-DC", "SAIL-WP-3X5-B-SAND", "COVER-CV-DCS-XL", "EH-15M-PA250A", "XMAS-LED-800-IC-UW", "XMAS-LED-800-IC-MC", "CAR-GAUGE-BK", "COVER-CV-DCS-L", "SAIL-WP-3X4-A-SAND",
#     ]
