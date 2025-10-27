
from app.integrations.shopify.shopify_client import ShopifyClient

if __name__ == "__main__":
    cli = ShopifyClient()
    data = cli.ping()
    print(data)


# 运行
# export $(grep -v '^#' .env | xargs)   # 若你用 .env
# python scripts/ping_shopify.py 



# 看到返回 shop.name / myshopifyDomain / plan.displayName 说明域名、版本、token 都 OK
