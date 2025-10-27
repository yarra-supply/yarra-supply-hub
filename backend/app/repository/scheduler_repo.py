# schedule database repository

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, Literal, Optional

from sqlalchemy import insert, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db.model.schedule import Schedule


Dow = Literal["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]
_ALLOWED_DOW = {"MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"}


@dataclass(slots=True)
class ScheduleUpsertDTO:
    enabled: bool
    day_of_week: Dow
    hour: int
    minute: int
    every_2_weeks: bool = True
    timezone: str = "Australia/Sydney"


# ---------- Query ----------
def list_all(db: Session) -> list[Schedule]:
    stmt = select(Schedule).order_by(Schedule.key.asc())
    return list(db.scalars(stmt))


def get(db: Session, key: str) -> Optional[Schedule]:
    stmt = select(Schedule).where(Schedule.key == key)
    return db.scalars(stmt).first()


# ---------- Mutations ----------
def upsert(db: Session, key: str, dto: ScheduleUpsertDTO) -> Schedule:
    """有则更新，无则插入。"""
    _validate(dto)
    now = datetime.now(timezone.utc)

    upd = (
        update(Schedule)
        .where(Schedule.key == key)
        .values(
            enabled=dto.enabled,
            day_of_week=dto.day_of_week,
            hour=dto.hour,
            minute=dto.minute,
            every_2_weeks=dto.every_2_weeks,
            timezone=dto.timezone,
            updated_at=now,
        )
    )
    res = db.execute(upd)
    if res.rowcount:
        db.commit()
        row = get(db, key)
        assert row is not None
        return row

    try:
        ins = insert(Schedule).values(
            key=key,
            enabled=dto.enabled,
            day_of_week=dto.day_of_week,
            hour=dto.hour,
            minute=dto.minute,
            every_2_weeks=dto.every_2_weeks,
            timezone=dto.timezone,
            created_at=now,
            updated_at=now,
        )
        db.execute(ins)
        db.commit()
    except IntegrityError:
        db.rollback()
        db.execute(upd)
        db.commit()

    row = get(db, key)
    if row is None:
        raise RuntimeError(f"failed to upsert schedule {key}")
    return row


def update_partial(db: Session, key: str, **fields) -> Optional[Schedule]:
    """
    仅更新给定字段；不存在返回 None。
    允许字段：enabled, day_of_week, hour, minute, every_2_weeks, timezone
    """
    allowed = {"enabled", "day_of_week", "hour", "minute", "every_2_weeks", "timezone"}
    clean = {k: v for k, v in fields.items() if k in allowed}
    if not clean:
        return get(db, key)

    if "day_of_week" in clean:
        _validate_dow(clean["day_of_week"])
    if "hour" in clean:
        _validate_hour(clean["hour"])
    if "minute" in clean:
        _validate_minute(clean["minute"])

    clean["updated_at"] = datetime.now(timezone.utc)

    stmt = update(Schedule).where(Schedule.key == key).values(**clean)
    res = db.execute(stmt)
    if not res.rowcount:
        db.rollback()
        return None
    db.commit()
    return get(db, key)


# ---------- Defaults ----------
def list_all_with_defaults(
    db: Session, defaults: Dict[str, ScheduleUpsertDTO]
) -> Iterable[Schedule]:
    """
    返回数据库内已有的定时任务；对于缺失的 key，用内置默认值拼返（不会写库）。
    """
    existing = {row.key: row for row in list_all(db)}
    # 保持固定顺序
    ordered_keys = sorted(defaults.keys())

    result: list[Schedule] = []
    for key in ordered_keys:
        row = existing.get(key)
        if row is not None:
            result.append(row)
            continue

        dto = defaults[key]
        _validate(dto)
        # 构造一个临时的 Schedule 实例（未持久化），返回给调用方展示
        result.append(
            Schedule(
                key=key,
                enabled=dto.enabled,
                day_of_week=dto.day_of_week,
                hour=dto.hour,
                minute=dto.minute,
                every_2_weeks=dto.every_2_weeks,
                timezone=dto.timezone,
                created_at=datetime.now(timezone.utc),
                updated_at=None,
            )
        )

    # 也包括数据库中存在但不在 defaults 列表的其他键
    for key, row in existing.items():
        if key not in defaults:
            result.append(row)

    return result


# ---------- Validation ----------
def _validate_dow(dow: str) -> None:
    if dow not in _ALLOWED_DOW:
        raise ValueError(f"day_of_week must be one of {sorted(_ALLOWED_DOW)}")


def _validate_hour(hour: int) -> None:
    if not isinstance(hour, int) or not (0 <= hour <= 23):
        raise ValueError("hour must be an integer in [0, 23]")


def _validate_minute(minute: int) -> None:
    if not isinstance(minute, int) or not (0 <= minute <= 59):
        raise ValueError("minute must be an integer in [0, 59]")


def _validate(dto: ScheduleUpsertDTO) -> None:
    _validate_dow(dto.day_of_week)
    _validate_hour(dto.hour)
    _validate_minute(dto.minute)
