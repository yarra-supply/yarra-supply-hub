// 运费计算参数配置页面

import { useEffect, useMemo, useState, type CSSProperties } from 'react';
import { Card, Tabs, Row, Col, Form, InputNumber, Space, Button, Affix, Typography, Tooltip, message } from 'antd';
import { SaveOutlined, ReloadOutlined, InfoCircleOutlined } from '@ant-design/icons';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { getFreightConfig, updateFreightConfig } from '@/api/freightConfig';
import type { FreightConfig } from '@/types/freightConfig';

const { Text, Title } = Typography;

type FreightConfigKey = Extract<keyof FreightConfig, string>;

type FieldDef = {
  name: FreightConfigKey;
  label: string;
  tip?: string;
  min?: number;
  max?: number;
  step?: number;
  // 显示单位，仅用于展示（通过 formatter/parser）
  suffix?: string; // '$' | '×' | '%'
};

type Section = {
  key: string;
  title: string;
  fields: FieldDef[];
};

const SECTIONS: Section[] = [
  {
    key: 'adjust',
    title: 'Adjust',
    fields: [
      { name: 'adjust_threshold', label: 'threshold', tip: '触发调价的下限价格', min: 0, step: 0.1, suffix: '$' },
      { name: 'adjust_rate', label: 'rate', tip: '调价倍率（0.04 表示 4%）', min: 0, max: 1, step: 0.001, suffix: '×' },
    ],
  },
  {
    key: 'remote',
    title: 'Remote',
    fields: [
      { name: 'remote_1', label: 'remote_1', min: 0, step: 1 },
      { name: 'remote_2', label: 'remote_2', min: 0, step: 1 },
      { name: 'wa_r', label: 'wa_r', min: 0, step: 1 },
    ],
  },
  {
    key: 'WeightedAveS',
    title: 'WeightedAveS',
    fields: [
      { name: 'weighted_ave_shipping_weights', label: 'shipping_weights', min: 0, max: 1, step: 0.01, suffix: '×' },
      { name: 'weighted_ave_rural_weights', label: 'rural_weights', min: 0, max: 1, step: 0.01, suffix: '×' },
    ],
  },
  {
    key: 'cubic',
    title: 'Cubic',
    fields: [
      { name: 'cubic_factor', label: 'factor', min: 0, step: 1 },
      { name: 'cubic_headroom', label: 'headroom', min: 0, step: 0.01, suffix: '×' },
    ],
  },
  {
    key: 'state_thresholds',
    title: 'State Thresholds',
    fields: [
      { name: 'price_ratio', label: 'price_ratio', min: 0, step: 0.001, suffix: '×' },
      { name: 'med_dif_10', label: 'med_dif_10', min: 0, step: 0.01 },
      { name: 'med_dif_20', label: 'med_dif_20', min: 0, step: 0.01 },
      { name: 'med_dif_40', label: 'med_dif_40', min: 0, step: 0.01 },
      { name: 'same_shipping_0', label: 'same_shipping_0', min: 0, step: 0.01 },
      { name: 'same_shipping_10', label: 'same_shipping_10', min: 0, step: 0.01 },
      { name: 'same_shipping_20', label: 'same_shipping_20', min: 0, step: 0.01 },
      { name: 'same_shipping_30', label: 'same_shipping_30', min: 0, step: 0.01 },
      { name: 'same_shipping_50', label: 'same_shipping_50', min: 0, step: 0.01 },
      { name: 'same_shipping_100', label: 'same_shipping_100', min: 0, step: 0.01 },
    ],
  },

  {
    key: 'shopify',
    title: 'Shopify',
    fields: [
      //todo sufix ？？？
      { name: 'shopify_threshold', label: 'threshold', tip: '价格阈值（用于低/高倍率分段）', min: 0, step: 0.01, suffix: '$' },
      { name: 'shopify_config1', label: 'shopify_config1', tip: '低于阈值的加价倍率', min: 0, step: 0.001, suffix: '×' },
      { name: 'shopify_config2', label: 'shopify_config2', tip: '高于阈值的加价倍率', min: 0, step: 0.001, suffix: '×' },
    ],
  },
  {
    key: 'kogan_au',
    title: 'Kogan AU',
    fields: [
      { name: 'kogan_au_normal_low_denom', label: 'normal_low_denom', tip: '低段分母', min: 0, step: 0.0001 },
      { name: 'kogan_au_normal_high_denom', label: 'normal_high_denom', tip: '高段分母', min: 0, step: 0.0001 },
      { name: 'kogan_au_extra5_discount', label: 'extra5_discount', tip: '额外 5% 折扣（乘数）', min: 0, step: 0.0001, suffix: '×' },
      { name: 'kogan_au_vic_half_factor', label: 'vic_half_factor', tip: 'VIC 半价系数', min: 0, max: 1, step: 0.01, suffix: '×' },
    ],
  },
  {
    key: 'k1',
    title: 'K1',
    fields: [
      { name: 'k1_threshold', label: 'threshold', min: 0, step: 0.01, suffix: '$' },
      { name: 'k1_discount_multiplier', label: 'discount_multiplier', min: 0, step: 0.0001, suffix: '×' },
      { name: 'k1_otherwise_minus', label: 'otherwise_minus', min: 0, step: 0.01, suffix: '$' },
    ],
  },
  {
    key: 'kogan_nz',
    title: 'Kogan NZ',
    fields: [
      { name: 'kogan_nz_service_no', label: 'kogan_nz_service_no', min: 0, step: 1 },
      { name: 'kogan_nz_config1', label: 'kogan_nz_config1', min: 0, max: 1, step: 0.001, suffix: '×' },
      { name: 'kogan_nz_config2', label: 'kogan_nz_config2', min: 0, max: 1, step: 0.001, suffix: '×' },
      { name: 'kogan_nz_config3', label: 'kogan_nz_config3', min: 0, step: 0.0001, suffix: '×' },
    ],
  },
  {
    key: 'update_weight',
    title: 'Weight',
    fields: [
      { name: 'weight_calc_divisor', label: 'weight_calc_divisor', min: 0, step: 1 },
      { name: 'weight_tolerance_ratio', label: 'weight_tolerance_ratio', min: 0, max: 1, step: 0.001, suffix: '×' },
    ],
  },
  
];

const inputStyle: CSSProperties = { width: 200 };

function withSuffixFormatter(suffix?: string) {
  return suffix
    ? {
        formatter: (v?: string | number) =>
          v === undefined || v === '' ? '' : `${v} ${suffix}`,
        parser: (v?: string) => (v ? v.replace(/[^\d.-]/g, '') : ''),
      }
    : {};
}

export default function FreightCalParamConfig() {
  const [form] = Form.useForm<FreightConfig>();
  const qc = useQueryClient();
  const [msg, ctx] = message.useMessage();
  const [saving, setSaving] = useState(false);

  const { data, refetch } = useQuery({
    queryKey: ['freight-config'],
    queryFn: getFreightConfig,
  });

  useEffect(() => {
    if (data) form.setFieldsValue(data);
  }, [data, form]);

  // 生成 patch：只提交变更字段
  const makePatch = (latest: FreightConfig): Partial<FreightConfig> => {
    const orig = data || ({} as FreightConfig);
    const diff: Partial<FreightConfig> = {};
    (Object.keys(latest) as (keyof FreightConfig)[]).forEach((k) => {
      if (latest[k] !== orig[k]) diff[k] = latest[k];
    });
    return diff;
  };

  const onSave = async () => {
    try {
      const values = await form.validateFields();
      const patch = makePatch(values);
      if (Object.keys(patch).length === 0) {
        msg.info('no change to be saved');
        return;
      }
      setSaving(true);
      const saved = await updateFreightConfig(patch);
      await qc.setQueryData(['freight-config'], saved);
      form.setFieldsValue(saved);
      msg.success('saved');
    } catch (e) {
      // 校验失败或请求异常
    } finally {
      setSaving(false);
    }
  };

  const onReset = () => {
    if (data) form.setFieldsValue(data);
  };

  const items = useMemo(
    () =>
      SECTIONS.map((section) => ({
        key: section.key,
        label: section.title,
        children: (
          <Card bordered={false}>
            <Row gutter={[16, 16]}>
              {section.fields.map((f) => (
                <Col key={String(f.name)} xs={24} sm={12} md={12} lg={8} xl={6}>
                  <Form.Item
                    name={f.name}
                    label={
                      <Space size={6}>
                        <Text strong>{f.label}</Text>
                        {f.tip && (
                          <Tooltip title={f.tip}>
                            <InfoCircleOutlined />
                          </Tooltip>
                        )}
                      </Space>
                    }
                    rules={[{ required: true, message: '必填' }]}
                  >
                    <InputNumber
                      {...withSuffixFormatter(f.suffix)}
                      style={inputStyle}
                      min={f.min}
                      max={f.max}
                      step={f.step}
                      controls
                    />
                  </Form.Item>
                </Col>
              ))}
            </Row>
          </Card>
        ),
      })),
    []
  );

  return (
    <Card title={<Title level={4} style={{ margin: 0 }}>Freight Config</Title>} bordered={false}>
      {ctx}
      <Form form={form} layout="vertical">
        <Tabs items={items} destroyInactiveTabPane={false} />
      </Form>

      <Affix offsetBottom={0}>
        <Card
          style={{
            borderTop: '1px solid #f0f0f0',
            borderRadius: 0,
            boxShadow: '0 -6px 12px rgba(0,0,0,0.06)',
          }}
        >
          <Space>
            <Button type="primary" icon={<SaveOutlined />} onClick={onSave} loading={saving}>
              Save
            </Button>
            <Button icon={<ReloadOutlined />} onClick={() => refetch()}>
              Reload
            </Button>
            <Button onClick={onReset}>Reset Unsaved changes</Button>
          </Space>
        </Card>
      </Affix>
    </Card>
  );
}
