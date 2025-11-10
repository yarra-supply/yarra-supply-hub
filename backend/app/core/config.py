# 环境变量和配置 
# pydantic‑settings 读取 .env = core/config.py

from typing import Optional, List
from pydantic import SecretStr, Field, AnyHttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Optional


# 本机直接跑 uvicorn 时（不走 Docker），才会用到 config.py 里的 model_config.env_file=".env"：
# 此时它会读取 backend/.env

class Settings(BaseSettings):

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        env_prefix="",
        case_sensitive=False,
    )

    # ========= project config =========
    PROJECT_NAME: str = "Yarra Supply Hub"
    ENVIRONMENT: str = "dev"
    API_PREFIX: str = "/api/v1"
    NEED_UPDATE_K1_SKUS_FILE: Optional[str] = Field(
        default=None,
        alias="NEED_UPDATE_K1_SKUS_FILE",
        description="Override path for need-update K1 SKU list (defaults to backend/data/need_update_k1_skus.txt).",
    )


    # ========= 登录 / 鉴权 / CORS =========
    SECRET_KEY: str = Field("CHANGE_ME", alias="SECRET_KEY")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = Field(480, alias="ACCESS_TOKEN_EXPIRE_MINUTES")   # 8h
    COOKIE_NAME: str = Field("access_token", alias="COOKIE_NAME")
    COOKIE_DOMAIN: Optional[str] = Field(None, alias="COOKIE_DOMAIN")
    COOKIE_SECURE: bool = Field(False, alias="COOKIE_SECURE")                            # 生产时改 True
    CORS_ORIGINS: List[AnyHttpUrl] = Field(default_factory=list, alias="CORS_ORIGINS")   # 统一用数组形式配置 CORS 源，避免逗号串再手动 split
    BACKEND_CORS_ORIGINS: str = "http://localhost:5173"


    # ========= Database =========
    # 说明：
    # - 容器内默认连 docker 网络里的 "db" 服务（见 docker-compose.yml）
    # - 本机工具（DBeaver/psql/脚本）可使用 DATABASE_URL_LOCAL（指向 localhost）
    DATABASE_URL: str = Field(                      #  改为 Field(...) 并注释说明
        default="postgresql+psycopg://ys_user:ys_pass@db:5432/yarra_dev",
        alias="DATABASE_URL"
    )
    DATABASE_URL_LOCAL: Optional[str] = Field(      #  新增：给 DBeaver/脚本使用
        default=None,
        alias="DATABASE_URL_LOCAL",
        description="Optional local URL for tools (e.g., DBeaver/psql). Typically '...@localhost:5432/yarra_dev'"
    )
    # todo
    REDIS_URL: Optional[str] = None


    # ========= celery config =========
    CELERY_BROKER_URL: Optional[str] = None
    CELERY_RESULT_BACKEND: Optional[str] = None
    CELERY_TIMEZONE: str = "Australia/Melbourne"
    CRON_WED_RESET: str = "0 20 * * WED"      # 定时cron表达式: 周三晚回原价（示例）
    CRON_THU_SYNC: str = "15 8 * * THU"       # 周四 8:15 全量同步 + 触发运费计算
    CRON_RETRY_SWEEPER: str = "*/5 * * * *"   # 失败重试扫表
    SYNC_TASKS_INLINE: bool = Field(default=True, alias="SYNC_TASKS_INLINE")
    SYNC_BULK_PREVIEW_LIMIT: int = Field(default=200, alias="SYNC_BULK_PREVIEW_LIMIT")


    # ========= reset price config =========
    PRICE_RESET_DB_PAGE: int = Field(default=1000, alias="PRICE_RESET_DB_PAGE")
    SHOPIFY_WRITE_CHUNK: int = Field(default=200, alias="SHOPIFY_WRITE_CHUNK")
    KOGAN_PRICE_KEY: str = Field(default="KoganAUPrice", alias="KOGAN_PRICE_KEY")
    KOGAN_NAMESPACE: str = Field(default="custom", alias="KOGAN_NAMESPACE")
    KOGAN_PRICE_TYPE: str = Field(default="number_decimal", alias="KOGAN_PRICE_TYPE")
     # 周三恢复原价：仅保留“每批变体数” —
    restore_chunk_variants: int = Field(20, description="每批更新的变体个数（一次会写 2×N 个 metafields）")


    # 若不填则默认用 broker；用于 Redis 计数器模式
    # todo 
    redis_url: str | None = None   # True=chord；False=Redis计数器 todo
    use_chord: bool = True                   


    # ========= DSZ Base Config =========
    DSZ_BASE_URL: str = Field("https://api.dropshipzone.com.au", alias="DSZ_BASE_URL", description="DSZ API base URL")
    DSZ_API_EMAIL: Optional[str] = Field(None, alias="DSZ_API_EMAIL")
    DSZ_API_PASSWORD: Optional[str] = Field(None, alias="DSZ_API_PASSWORD")
    # DSZ_API_TOKEN: Optional[str] = Field(None, alias="DSZ_API_TOKEN")                       # 方式二（可选）：预置 token（测试/临时），为空则走 /auth
    DSZ_RATE_LIMIT_PER_MIN: int = Field(100, ge=1, le=600, alias="DSZ_RATE_LIMIT_PER_MIN")    # 每分钟 100
    DSZ_CONNECT_TIMEOUT: int = Field(10, ge=1, alias="DSZ_CONNECT_TIMEOUT")
    DSZ_READ_TIMEOUT: int = Field(30, ge=1, alias="DSZ_READ_TIMEOUT")

    DSZ_TOKEN_TTL_SEC: int = Field(15 * 60, ge=60, alias="DSZ_TOKEN_TTL_SEC")                 # /auth 未含 exp 时的兜底 TTL
    DSZ_DETAIL_LIST_MAX_PER_CHUNK: int = 300

    # ========= DSZ product API config =========
    DSZ_PRODUCTS_ENDPOINT: str = "/v2/products"
    DSZ_PRODUCTS_METHOD: str = "GET"         # "POST" 或 "GET"
    DSZ_PRODUCTS_SKU_PARAM: str = "skus"
    DSZ_PRODUCTS_MAX_PER_REQ: int = 50
    DSZ_PRODUCTS_SKU_FIELD: str = "sku"

    # Zone rates 配置
    DSZ_ZONE_RATES_ENDPOINT: str = "/v2/get_zone_rates"
    DSZ_ZONE_RATES_METHOD: str = "POST"   # 文档是 POST
    DSZ_ZONE_RATES_LIMIT: int = 160       # 官方上限 160

    # ========= DSZ 全局限流配置 =========
    DSZ_GLOBAL_RL_ENABLED: bool = True      # 是否启用全局限流
    DSZ_GLOBAL_RATE_LIMIT_REDIS_URL: str = "redis://redis:6379/0"
    DSZ_GLOBAL_RL_MAX_RPM: int = 100  # 每分钟最大速率（rate）
    DSZ_GLOBAL_RL_BURST: int = 5     # 桶容量
    DSZ_GLOBAL_RL_KEY_PREFIX: str = "dsz:rl"
    DSZ_ENV: str = "dev"  # 用于区分不同环境拼 key



    # todo
    # ========= Shopify API Config =========
    SHOPIFY_SHOP: str = Field("yarra-supply.myshopify.com", alias="SHOPIFY_SHOP")
    SHOPIFY_ADMIN_TOKEN: Optional[SecretStr] = Field(None, alias="SHOPIFY_ADMIN_TOKEN")            # 必须在运行时填上真实值
    SHOPIFY_API_VERSION: str = Field("2025-07", alias="SHOPIFY_API_VERSION")
    BULK_POLL_INTERVAL_SEC: int = Field(8, ge=5, le=60, alias="SHOPIFY_BULK_POLL_INTERVAL_SEC")    # 轮询间隔（也可只用 webhook）
    BULK_DOWNLOAD_TIMEOUT: int = Field(180, ge=30, le=300, alias="SHOPIFY_BULK_DOWNLOAD_TIMEOUT")  # 下载 JSONL 超时秒数
    # webhook 配置
    SHOPIFY_WEBHOOK_HOST: Optional[str] = Field(None, alias="SHOPIFY_WEBHOOK_HOST")
    SHOPIFY_WEBHOOK_SECRET: Optional[str] = Field(None, alias="SHOPIFY_WEBHOOK_SECRET")
    SHOPIFY_TAG_FULL_SYNC: str = Field(default="DropshipzoneAU", alias="SHOPIFY_TAG_FULL_SYNC")


    # 网络/HTTP 层 配置 测试时调参
    SHOPIFY_HTTP_TIMEOUT: int = Field(30, alias="SHOPIFY_HTTP_TIMEOUT")
    SHOPIFY_HTTP_RETRIES: int = Field(3, alias="SHOPIFY_HTTP_RETRIES")
    SHOPIFY_HTTP_BACKOFF_MS: int = Field(200, alias="SHOPIFY_HTTP_BACKOFF_MS")
    SHOPIFY_BULK_START_RETRIES: int = Field(3, alias="SHOPIFY_BULK_START_RETRIES")     # 业务级 Bulk 发起重试
    # todo ?
    SHOPIFY_DISPATCH_BATCH: int = Field(20, ge=10, le=20, alias="SHOPIFY_DISPATCH_BATCH")  # 合并 10~20
    SHOPIFY_TAG_FULL_SYNC: str = Field(default="DropShippingZone", alias="SHOPIFY_TAG_FULL_SYNC")
    SHOPIFY_BULK_TEST_MODE: bool = Field(default=True, alias="SHOPIFY_BULK_TEST_MODE")


    # 批处理窗口（内存）
    PROCESSING_WINDOW: int = 1000      # 500~1000
    CHORD_SPLIT_AT: int = 200          # 单个 chord 的最大 header 数量，超出则分层
    SYNC_CHUNK_SKUS: int = 5000        # todo 测试修改
    FREIGHT_BATCH_SIZE: int = Field(1000, alias="FREIGHT_BATCH_SIZE")
    

    # todo 需要写吗？）
    # ========= Shopify Metafields Config（默认 number_decimal） =========
    kogan_namespace: str = "market"
    kogan_price_key: str = "kogan_au_price"
    kogan_price_type: str = "number_decimal"
    ebay_namespace: str = "market"
    ebay_price_key: str = "ebay_price"
    ebay_price_type: str = "number_decimal"


    # todo 队列和路由 获取“做计数器/限流等业务键值”要用的 Redis 地址
    @property
    def redis_for_counters(self) -> str:
        return self.redis_url or self.CELERY_BROKER_URL


settings = Settings()  # 只从环境读取（含 .env）
