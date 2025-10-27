
#!/usr/bin/env python3
from __future__ import annotations
import os, sys, json, argparse
from backend.app.integrations.shopify.shopify_client import ShopifyClient


'''
这是一个运维小脚本（命令行工具），方便在 CI/CD 或本地把 Webhook 订阅配好
    - 从命令行拿 --callback（或读取环境变量 SHOPIFY_WEBHOOK_CALLBACK）
    - 然后调用上面的 ensure_bulk_finish_webhook
    - 使用场景：在部署前或切环境时调用 register_bulk_webhook.py 
    - 用法： 
    python register_bulk_webhook.py \
    --callback "https://<your-public-domain>/webhooks/shopify/bulk_operations/finish" \
    --delete-others
'''
# todo 测试
def main():
    ap = argparse.ArgumentParser(description="Ensure Shopify BULK_OPERATIONS_FINISH webhook subscription.")
    ap.add_argument("--callback", help="Public HTTPS callback URL, e.g. https://xxxx.ngrok.io/webhooks/shopify/bulk_operations/finish")
    ap.add_argument("--delete-others", action="store_true", default=True, help="Delete other callbacks for the same topic (default: true)")
    args = ap.parse_args()

    callback = args.callback or os.getenv("SHOPIFY_WEBHOOK_CALLBACK")
    if not callback:
        print("ERROR: provide --callback or set SHOPIFY_WEBHOOK_CALLBACK", file=sys.stderr)
        sys.exit(2)

    client = ShopifyClient()
    result = client.ensure_bulk_finish_webhook(callback, delete_others=args.delete_others)
    print(json.dumps(result, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
