# 定时任务配置相关接口 -> 前端产品页面调用
from typing import Dict, List, Literal

from fastapi import APIRouter, Depends, HTTPException, Path
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.repository.scheduler_repo import ScheduleUpsertDTO, list_all_with_defaults, upsert
from app.services.auth_service import get_current_user

router = APIRouter(
    prefix="/schedules",
    tags=["schedules"],
    dependencies=[Depends(get_current_user)],
)

# 固定两条规则，也支持后续扩展
ScheduleKey = Literal["price_reset", "product_full_sync"]


class ScheduleItem(BaseModel):
    key: ScheduleKey
    enabled: bool
    day_of_week: Literal["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]
    hour: int = Field(ge=0, le=23)
    minute: int = Field(ge=0, le=59)
    every_2_weeks: bool
    timezone: str
    updated_at: str | None = None


class ScheduleUpsert(BaseModel):
    enabled: bool
    day_of_week: Literal["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]
    hour: int = Field(ge=0, le=23)
    minute: int = Field(ge=0, le=59)
    every_2_weeks: bool = True
    timezone: str = "Australia/Sydney"


@router.get("", response_model=List[ScheduleItem])
def list_schedules(db: Session = Depends(get_db)) -> List[ScheduleItem]:
    """
    返回所有定时任务配置。
    若表中缺失某个 key，则直接用内置默认值（但不写库）。
    """
    rows = list_all_with_defaults(db, _DEFAULTS_DTO)
    return [_to_item(row) for row in rows]


@router.put("/{key}", response_model=ScheduleItem)
def upsert_schedule(
    key: ScheduleKey = Path(..., description="price_reset | product_full_sync"),
    body: ScheduleUpsert = ...,
    db: Session = Depends(get_db),
) -> ScheduleItem:
    if key not in _DEFAULTS_DTO:
        raise HTTPException(status_code=400, detail="unsupported schedule key")

    try:
        row = upsert(
            db,
            key,
            ScheduleUpsertDTO(
                enabled=body.enabled,
                day_of_week=body.day_of_week,
                hour=body.hour,
                minute=body.minute,
                every_2_weeks=body.every_2_weeks,
                timezone=body.timezone,
            ),
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    return _to_item(row)


_DEFAULTS_DTO: Dict[ScheduleKey, ScheduleUpsertDTO] = {
    "price_reset": ScheduleUpsertDTO(
        enabled=False,
        day_of_week="WED",
        hour=20,
        minute=0,
        every_2_weeks=True,
        timezone="Australia/Sydney",
    ),
    "product_full_sync": ScheduleUpsertDTO(
        enabled=False,
        day_of_week="THU",
        hour=8,
        minute=10,
        every_2_weeks=True,
        timezone="Australia/Sydney",
    ),
}


def _to_item(row) -> ScheduleItem:
    return ScheduleItem(
        key=row.key,  # type: ignore[arg-type]
        enabled=row.enabled,
        day_of_week=row.day_of_week,  # type: ignore[arg-type]
        hour=row.hour,
        minute=row.minute,
        every_2_weeks=row.every_2_weeks,
        timezone=row.timezone,
        updated_at=row.updated_at.isoformat() if getattr(row, "updated_at", None) else None,
    )
