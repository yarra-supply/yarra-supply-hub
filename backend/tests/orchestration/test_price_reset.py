import uuid
from datetime import date
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.model.freight import SkuFreightFee
from app.db.model.product import SkuInfo
from app.db.session import SessionLocal
from app.orchestration.price_reset import price_reset
from app.repository import product_repo
from app.services.freight.freight_compute import (
    FreightInputs as FreightComputeInputs,
    compute_all,
)


pytestmark = pytest.mark.integration


# 准备一套固定的运费字段值（各州运费/remote/NZ 等），确保测试时的输入稳定，方便 debug。
# 这里的数值全部写死，直接写入真实数据库
def _state_snapshot_for_test() -> dict[str, Decimal]:
    """Provide a deterministic freight snapshot for debug assertions."""
    return {
        "freight_act": Decimal("12.50"),
        "freight_nsw_m": Decimal("13.20"),
        "freight_nsw_r": Decimal("14.90"),
        "freight_qld_m": Decimal("13.70"),
        "freight_qld_r": Decimal("15.10"),
        "freight_sa_m": Decimal("11.85"),
        "freight_sa_r": Decimal("12.40"),
        "freight_tas_m": Decimal("16.35"),
        "freight_tas_r": Decimal("17.25"),
        "freight_vic_m": Decimal("10.75"),
        "freight_vic_r": Decimal("12.10"),
        "freight_wa_m": Decimal("19.80"),
        "freight_wa_r": Decimal("21.45"),
        "remote": Decimal("25.30"),
        "freight_nz": Decimal("29.90"),
    }


# 把这套运费数据和价格写入 sku_info 与 kogan_sku_freight_fee 表中，
# 等于手工造出一条满足「明天特价到期」条件的 SKU。调用后数据库里就存在一条真实的候选记录。
def _setup_sku_record(db: Session, *, sku_code: str, base_price: Decimal, freight_vals: dict[str, Decimal], target_date: date) -> None:
    """Insert sku_info + kogan_sku_freight_fee fixture rows for the integration run."""

    # 这三行是在测试前先把数据库里同名 SKU 的旧数据清掉，避免和我们造的测试数据冲突。
    # db.flush() 会把这次删除操作立刻同步到数据库（还没 commit，但当前事务里已经生效），确保后面插入时不会被唯一键之类的问题挡住
    db.query(SkuFreightFee).filter(SkuFreightFee.sku_code == sku_code).delete()
    db.query(SkuInfo).filter(SkuInfo.sku_code == sku_code).delete()
    db.flush()

    sku = SkuInfo(
        sku_code=sku_code,
        price=base_price,
        special_price=base_price - Decimal("5.00"),
        special_price_end_date=target_date,
        weight=Decimal("2.40"),
        attrs_hash_current="fixture-price-reset",
        freight_act=freight_vals["freight_act"],
        freight_nsw_m=freight_vals["freight_nsw_m"],
        freight_nsw_r=freight_vals["freight_nsw_r"],
        freight_qld_m=freight_vals["freight_qld_m"],
        freight_qld_r=freight_vals["freight_qld_r"],
        freight_sa_m=freight_vals["freight_sa_m"],
        freight_sa_r=freight_vals["freight_sa_r"],
        freight_tas_m=freight_vals["freight_tas_m"],
        freight_tas_r=freight_vals["freight_tas_r"],
        freight_vic_m=freight_vals["freight_vic_m"],
        freight_vic_r=freight_vals["freight_vic_r"],
        freight_wa_m=freight_vals["freight_wa_m"],
        freight_wa_r=freight_vals["freight_wa_r"],
        remote=freight_vals["remote"],
        freight_nz=freight_vals["freight_nz"],
    )
    db.add(sku)

    fee = SkuFreightFee(
        sku_code=sku_code,
        selling_price=Decimal("0.00"),
        kogan_au_price=Decimal("0.00"),
        kogan_k1_price=Decimal("0.00"),
        kogan_nz_price=Decimal("0.00"),
        remote_check=False,
        kogan_dirty=False,
    )
    db.merge(fee)
    db.commit()



def _expected_snapshot(base_price: Decimal, freight_vals: dict[str, Decimal]) -> dict[str, Decimal]:
    """Reuse the production compute logic to derive expected 2-decimal values."""
    state_inputs = {
        "ACT": float(freight_vals["freight_act"]),
        "NSW_M": float(freight_vals["freight_nsw_m"]),
        "NSW_R": float(freight_vals["freight_nsw_r"]),
        "QLD_M": float(freight_vals["freight_qld_m"]),
        "QLD_R": float(freight_vals["freight_qld_r"]),
        "SA_M": float(freight_vals["freight_sa_m"]),
        "SA_R": float(freight_vals["freight_sa_r"]),
        "TAS_M": float(freight_vals["freight_tas_m"]),
        "TAS_R": float(freight_vals["freight_tas_r"]),
        "VIC_M": float(freight_vals["freight_vic_m"]),
        "VIC_R": float(freight_vals["freight_vic_r"]),
        "WA_M": float(freight_vals["freight_wa_m"]),
        "WA_R": float(freight_vals["freight_wa_r"]),
        "REMOTE": float(freight_vals["remote"]),
        "NZ": float(freight_vals["freight_nz"]),
    }
    computed = compute_all(
        FreightComputeInputs(
            price=float(base_price),
            special_price=None,
            state_freight=state_inputs,
            weight=2.40,
            cbm=None,
        )
    )
    return {
        "selling_price": price_reset._q2(computed.selling_price),
        "kogan_au_price": price_reset._q2(computed.kogan_au_price),
        "kogan_k1_price": price_reset._q2(computed.kogan_k1_price),
        "kogan_nz_price": price_reset._q2(computed.kogan_nz_price),
    }




def test_kick_price_reset_runs_end_to_end(monkeypatch):
    """Run kick_price_reset() inline and inspect DB state for a synthetic SKU."""

    tz = ZoneInfo(getattr(settings, "CELERY_TIMEZONE", "Australia/Melbourne"))
    monkeypatch.setattr(price_reset, "_CELERY_TZ", tz, raising=False)

    # 生成随机 SKU 码 
    # sku_code = f"PR-INT-{uuid.uuid4().hex[:8]}"
    sku_code = "V420-CSST-WPOTGSET-C"
    base_price = Decimal("59.90")

    # 构建要写入的 运费 数据
    freight_vals = _state_snapshot_for_test()

    # 构建 期待的 结果数据
    expected = _expected_snapshot(base_price, freight_vals)


    # 是用 SQLAlchemy Session 的上下文管理器，进入块时拿到一个数据库连接，结束时自动关闭
    # 插入测试 SKU，并用迭代器确认能被选中
    with SessionLocal() as db:
        target_date = price_reset._tomorrow_local_date()

        # 写入sku_info + kogan_sku_freight_fee 数据
        _setup_sku_record(
            db,
            sku_code=sku_code,
            base_price=base_price,
            freight_vals=freight_vals,
            target_date=target_date,
        )

        # 查询 sku_info 历史数据
        # 调用 product_repo.iter_price_reset_candidates() 验证刚才插入的 SKU 会被「候选查询」捞到，并把候选列表打印出来，方便逐步查看。
        candidates = list(
            product_repo.iter_price_reset_candidates(
                db,
                target_date=target_date,
                page_size=10,
            )
        )
        print("[price_reset candidates]", candidates)
        assert any(row[0] == sku_code for row in candidates), "Fixture SKU not discoverable by iter_price_reset_candidates"

    # 整个任务使用真实 DB/真实 compute_all 逻辑
    result = price_reset.kick_price_reset.run()
    print("[kick_price_reset result]", result)

    assert isinstance(result, dict)
    assert result.get("error") is None


    # 运行任务后重新开一个 session 读取 kogan_sku_freight_fee 表，和期望值逐项比对
    # 读回 kogan_sku_freight_fee 更新后的行，把 4 个目标价格、脏标记、来源和 run_id 全部打印出来，并跟 compute_all 算出来的期望值逐一断言
    with SessionLocal() as verify:
        refreshed = verify.get(SkuFreightFee, sku_code)
        assert refreshed is not None, "Expected freight fee row missing after kick_price_reset"

        print("[price_reset updated row]", {
            "selling_price": refreshed.selling_price,
            "kogan_au_price": refreshed.kogan_au_price,
            "kogan_k1_price": refreshed.kogan_k1_price,
            "kogan_nz_price": refreshed.kogan_nz_price,
            "kogan_dirty": refreshed.kogan_dirty,
            "last_changed_source": refreshed.last_changed_source,
            "last_changed_run_id": refreshed.last_changed_run_id,
        })
        assert refreshed.selling_price == expected["selling_price"]
        assert refreshed.kogan_au_price == expected["kogan_au_price"]
        assert refreshed.kogan_k1_price == expected["kogan_k1_price"]
        assert refreshed.kogan_nz_price == expected["kogan_nz_price"]
        assert refreshed.kogan_dirty is True
        assert refreshed.last_changed_source == "price_reset"
        assert refreshed.last_changed_run_id is not None


    # 测试结束后把这条 SKU 清理掉，保持 DB 干净
    with SessionLocal() as cleanup:
        cleanup.query(SkuFreightFee).filter(SkuFreightFee.sku_code == sku_code).delete()
        cleanup.query(SkuInfo).filter(SkuInfo.sku_code == sku_code).delete()
        cleanup.commit()
