
import { Card, Typography } from 'antd';

export default function ShopifyJobsPage() {
  return (
    <div style={{ padding: 8 }}>
      <Typography.Title level={4}>Shopify 商品同步任务</Typography.Title>
      <Card>
        <Typography.Paragraph>
          {/* 这里用于展示 / 触发 Shopify 同步作业（占位）。后续可接入：作业队列、批次、状态、失败重试等。 */}
          This section is for showing or triggering Shopify sync jobs (placeholder). 
          Later plug in: job queue, batches, status, and retry on failures.
        </Typography.Paragraph>
      </Card>
    </div>
  );
}

