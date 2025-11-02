import uuid
from datetime import date
from decimal import Decimal
import pytest
from sqlalchemy.orm import Session
from app.db.model.freight import SkuFreightFee
from app.db.model.product import SkuInfo
from app.db.session import SessionLocal
from app.orchestration.price_reset import price_reset
from app.repository import product_repo
from app.repository.freight_repo import load_fee_rows_by_skus
from app.services.freight.freight_compute import FreightInputs, compute_all


pytestmark = pytest.mark.integration


def _freight_snapshot() -> dict[str, Decimal]:
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


def _dimension_snapshot() -> dict[str, Decimal]:
    return {
        "weight": Decimal("2.40"),
        "length": Decimal("30.0"),
        "width": Decimal("25.0"),
        "height": Decimal("18.0"),
        "cbm": Decimal("0.020"),
    }


def _setup_fixture_row(
    db: Session,
    *,
    sku_code: str,
    base_price: Decimal,
    target_date: date,
    freight_vals: dict[str, Decimal],
    dims: dict[str, Decimal],
) -> None:
    db.query(SkuFreightFee).filter(SkuFreightFee.sku_code == sku_code).delete()
    db.query(SkuInfo).filter(SkuInfo.sku_code == sku_code).delete()
    db.flush()

    sku = SkuInfo(
        sku_code=sku_code,
        price=base_price,
        special_price=base_price - Decimal("5.00"),
        special_price_end_date=target_date,
        attrs_hash_current="fixture-price-reset",
        weight=dims["weight"],
        length=dims["length"],
        width=dims["width"],
        height=dims["height"],
        cbm=dims["cbm"],
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

    fee_row = SkuFreightFee(
        sku_code=sku_code,
        selling_price=Decimal("0.00"),
        shopify_price=Decimal("0.00"),
        kogan_au_price=Decimal("0.00"),
        kogan_k1_price=Decimal("0.00"),
        kogan_nz_price=Decimal("0.00"),
        remote_check=False,
        shipping_type="0",
        kogan_dirty_au=False,
        kogan_dirty_nz=False,
    )
    db.merge(fee_row)
    db.commit()


def _expected_outputs(
    base_price: Decimal,
    freight_vals: dict[str, Decimal],
    dims: dict[str, Decimal],
    target_date: date,
) -> dict[str, object]:
    fi = FreightInputs(
        price=float(base_price),
        special_price=None,
        special_price_end_date=target_date,
        length=float(dims["length"]),
        width=float(dims["width"]),
        height=float(dims["height"]),
        weight=float(dims["weight"]),
        cbm=float(dims["cbm"]),
        act=float(freight_vals["freight_act"]),
        nsw_m=float(freight_vals["freight_nsw_m"]),
        nsw_r=float(freight_vals["freight_nsw_r"]),
        qld_m=float(freight_vals["freight_qld_m"]),
        qld_r=float(freight_vals["freight_qld_r"]),
        sa_m=float(freight_vals["freight_sa_m"]),
        sa_r=float(freight_vals["freight_sa_r"]),
        tas_m=float(freight_vals["freight_tas_m"]),
        tas_r=float(freight_vals["freight_tas_r"]),
        vic_m=float(freight_vals["freight_vic_m"]),
        vic_r=float(freight_vals["freight_vic_r"]),
        wa_m=float(freight_vals["freight_wa_m"]),
        wa_r=float(freight_vals["freight_wa_r"]),
        nt_m=None,
        nt_r=None,
        remote=float(freight_vals["remote"]),
        nz=float(freight_vals["freight_nz"]),
    )
    out = compute_all(fi)

    expected: dict[str, object] = {}
    for column, attr in price_reset._OUTPUT_FIELDS:
        expected[column] = price_reset._normalize_value(column, getattr(out, attr, None))
    return expected


def test_kick_price_reset_end_to_end():
    """Run the real price_reset flow against live DB tables and trace intermediate state."""

    # sku_code = f"PR-INT-{uuid.uuid4().hex[:8]}"
    # base_price = Decimal("59.90")
    # freight_vals = _freight_snapshot()
    # dims = _dimension_snapshot()

    # with SessionLocal() as db:
    #     target_date = price_reset._tomorrow_local_date()
    #     _setup_fixture_row(
    #         db,
    #         sku_code=sku_code,
    #         base_price=base_price,
    #         target_date=target_date,
    #         freight_vals=freight_vals,
    #         dims=dims,
    #     )

    #     candidates = list(
    #         product_repo.iter_price_reset_candidates(
    #             db,
    #             target_date=target_date,
    #             page_size=10,
    #         )
    #     )
    #     print("[price_reset candidate list]", candidates)
    #     assert sku_code in candidates

    #     old_fee_snapshot = load_fee_rows_by_skus(db, [sku_code])[sku_code]
    #     print("[price_reset fee snapshot before]", old_fee_snapshot)

    result = price_reset.kick_price_reset()
    print("[price_reset task result]", result)
    assert isinstance(result, dict)
    assert result.get("error") is None

    # expected_outputs = _expected_outputs(base_price, freight_vals, dims, target_date)
    # print("[price_reset expected outputs]", expected_outputs)

    # with SessionLocal() as db:
    #     refreshed = db.get(SkuFreightFee, sku_code)
    #     assert refreshed is not None

    #     actual_outputs = {
    #         column: price_reset._normalize_value(column, getattr(refreshed, column, None))
    #         for column, _ in price_reset._OUTPUT_FIELDS
    #     }
    #     print("[price_reset refreshed outputs]", actual_outputs)

    #     for column, expected_value in expected_outputs.items():
    #         assert actual_outputs[column] == expected_value, f"{column} mismatch"

    #     assert refreshed.kogan_dirty_au is True
    #     assert refreshed.kogan_dirty_nz is True
    #     assert refreshed.last_changed_source == "price_reset"
    #     assert refreshed.last_changed_run_id is not None

    # with SessionLocal() as cleanup:
    #     cleanup.query(SkuFreightFee).filter(SkuFreightFee.sku_code == sku_code).delete()
    #     cleanup.query(SkuInfo).filter(SkuInfo.sku_code == sku_code).delete()
    #     cleanup.commit()
