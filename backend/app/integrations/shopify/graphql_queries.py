import json


# GraphQL 片段
_LIST_WEBHOOKS = """
query ListWebhooks($first:Int!, $topic: WebhookSubscriptionTopic){
  webhookSubscriptions(first: $first, topics: [$topic]) {
    edges {
      node {
        id
        topic
        endpoint {
          __typename
          ... on WebhookHttpEndpoint { callbackUrl }
        }
      }
    }
  }
}
""".strip()


_CREATE_WEBHOOK = """
mutation CreateWebhook($topic: WebhookSubscriptionTopic!, $cb: URL!){
  webhookSubscriptionCreate(
    topic: $topic
    webhookSubscription: { callbackUrl: $cb, format: JSON }
  ){
    userErrors { field message }
    webhookSubscription {
      id
      topic
      endpoint { __typename ... on WebhookHttpEndpoint { callbackUrl } }
    }
  }
}
""".strip()


_DELETE_WEBHOOK = """
mutation DeleteWebhook($id: ID!){
  webhookSubscriptionDelete(id: $id){
    userErrors { field message }
    deletedWebhookSubscriptionId
  }
}
""".strip()


# 新增一个简易转义器，确保 tag 放入 query 字符串安全
def escape_tag_for_query(tag: str) -> str:
    """转义 tag 供 Shopify 搜索字符串使用，并统一包裹双引号。"""
    value = json.dumps(tag or "")[1:-1]
    return f'"{value}"'



# 读取型 Bulk（按标签 + 状态筛选）：
# 注意：Bulk 必须以 connection 开头（products），variants 也用 connection。
# todo 需要字段:
# product 层: id, vendor - 供应商
# productVariant 层: id（就是shopify_variant_id）, sku, barcode,
BULK_PRODUCTS_BY_TAG_AND_STATUS = r"""
{
  products(%(products_args)s) {
    edges {
      node {
        id
        vendor
        tags               # 产品标签字段
        variants%(variants_args)s {
          edges {
            node {
              id
              sku
              barcode
              price
              compareAtPrice
              inventoryItem {
                unitCost { amount currencyCode }
              }
            }
          }
        }
      }
    }
  }
}
"""


"""
测试用：限制到前 10 个商品、每个商品前 50 个变体。
格式化时依旧需要传入 {"filter": json.dumps(...)}。
"""
BULK_PRODUCTS_BY_TAG_AND_STATUS_TEST = BULK_PRODUCTS_BY_TAG_AND_STATUS % {
    "products_args": "first: 10, query: %(filter)s",
    "variants_args": "(first: 50)",
}

BULK_PRODUCTS_BY_TAG_AND_STATUS_TEST_LIMIT_20 = r"""
{
  products(first: 20, query: %(filter)s) {
    edges {
      node {
        id
        vendor
        variants%(variants_args)s {
          edges {
            node {
              id
              sku
              barcode
              price
              compareAtPrice
              inventoryItem {
                unitCost { amount currencyCode }
              }
            }
          }
        }
      }
    }
  }
}
""".strip()


# 使用时提供：{"products_first": int, "variants_args": "(first: X)" 或 "", "filter": json.dumps(search)}
BULK_PRODUCTS_BY_TAG_AND_STATUS_LIMITED = r"""
{
  products(first: %(products_first)d, query: %(filter)s) {
    edges {
      node {
        id
        vendor
        variants%(variants_args)s {
          edges {
            node {
              id
              sku
              barcode
              price
              compareAtPrice
              inventoryItem {
                unitCost { amount currencyCode }
              }
            }
          }
        }
      }
    }
  }
}
""".strip()


# 普通 GraphQL 查询：按标签拉取少量商品（默认 10 个）
PRODUCTS_BY_TAG_AND_STATUS = """
query ProductsByTagAndStatus($query: String!, $first: Int!, $variantsFirst: Int!) {
  products(first: $first, query: $query) {
    edges {
      node {
        id
        vendor
        variants(first: $variantsFirst) {
          edges {
            node {
              id
              sku
              barcode
              price
              compareAtPrice
              inventoryItem {
                unitCost { amount currencyCode }
              }
            }
          }
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
""".strip()
