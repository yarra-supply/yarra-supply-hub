

import { useState } from 'react';
import { Card, Col, Form, Row, Switch, TimePicker, Button, Space, message, Typography, Tag, Empty, Select } from 'antd';
import dayjs from 'dayjs';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getSchedules, updateSchedule } from '@/api/schedules';
import type { Schedule, Dow } from '@/types/schedule';


// 仅用于显示友好标题（不是默认值）
const LABELS: Record<string, string> = {
  price_reset: 'Price Reset',
  product_full_sync: 'Full Product Sync',
};

// 全周可选（显示中文 + 英文简称）
const DOW_OPTIONS: { label: string; value: Dow }[] = [
  { label: 'MON', value: 'MON' },
  { label: 'TUE', value: 'TUE' },
  { label: 'WED', value: 'WED' },
  { label: 'THU', value: 'THU' },
  { label: 'FRI', value: 'FRI' },
  { label: 'SAT', value: 'SAT' },
  { label: 'SUN', value: 'SUN' },
];

export default function SchedulesPage() {
  const qc = useQueryClient();

  // 查询后端定时任务配置接口
  const { data, isLoading } = useQuery({
    queryKey: ['schedules'],
    queryFn: getSchedules,
  });

  const list: Schedule[] = data ?? [];

  const mutation = useMutation({
    mutationFn: (payload: Schedule) => updateSchedule(payload.key, payload),
    onSuccess: () => {
      message.success('saved');
      qc.invalidateQueries({ queryKey: ['schedules'] });
    },
    onError: (e: any) => {
      message.error(e?.response?.data?.detail || 'Save Failed');
    }
  });

  const onSave = (s: Schedule, values: any) => {
    const t = values.time as dayjs.Dayjs;
    const payload: Schedule = {
      ...s,
      enabled: values.enabled,
      day_of_week: values.day_of_week,
      hour: t.hour(),
      minute: t.minute(),
      // 其余字段沿用后端返回的值
    };
    mutation.mutate(payload); // ✅ 只更这一条
  };


  return (
    <div style={{ padding: 8 }}>

      <Typography.Title level={4} style={{ marginBottom: 16 }}>
        Schedules Configuration
      </Typography.Title>
      <Typography.Paragraph type="secondary" style={{ marginTop: -8 }}>
        Price Reset Config: refresh promotion prices back to the original price.
        Full Product Sync Config: perform a full data sync according to the configured cadence.
      </Typography.Paragraph>

      {(!isLoading && list.length === 0) ? (
        <Empty description="暂无配置" />
      ) : (
        <Row gutter={[16, 16]}>
          {list.map((s) => (
            <Col span={12} key={s.key}>
              <Card
                loading={isLoading}
                title={
                  <Space>
                    <span>{s.label || LABELS[s.key] || s.key}</span>
                    <Tag color="blue">{s.day_of_week}</Tag>
                    <Tag>{s.every_2_weeks ? 'every two weeks' : 'every week'}</Tag>
                  </Space>
                }
              >
                {/* ✅ 关键：用值拼接成 key，保存后服务端数据刷新会触发重新挂载，从而重置 isDirty=false */}
                <ScheduleForm
                  key={`${s.key}-${s.enabled}-${s.day_of_week}-${s.hour}-${s.minute}`}
                  initial={s}
                  onSave={(v) => onSave(s, v)}
                  saving={mutation.isPending}
                />
              </Card>
            </Col>
          ))}
        </Row>
      )}

    </div>
  );
}



function ScheduleForm({ initial, onSave, saving }: 
  { initial: Schedule; onSave: (v: any) => void; saving: boolean }) {

  const [form] = Form.useForm();
  const [isDirty, setIsDirty] = useState(false);

  const baseline = {
    enabled: initial.enabled ?? false,
    day_of_week: initial.day_of_week,
    hour: initial.hour,
    minute: initial.minute,
  };

  const initTime = dayjs().hour(initial.hour).minute(initial.minute).second(0);

  // ✅ 表单变更比较基线：只有 enabled / time（hour/minute）参与比较
  const handleValuesChange = () => {
    const { enabled, day_of_week, time } = form.getFieldsValue();
    const h = dayjs.isDayjs(time) ? time.hour() : baseline.hour;
    const m = dayjs.isDayjs(time) ? time.minute() : baseline.minute;
    const changed =
      (enabled ?? false) !== baseline.enabled 
      ||(day_of_week ?? baseline.day_of_week) !== baseline.day_of_week
      || h !== baseline.hour || m !== baseline.minute;
    setIsDirty(changed);
  };



  return (
    <Form
      form={form}
      layout="vertical"
      initialValues={{ 
        enabled: initial.enabled, 
        day_of_week: initial.day_of_week, 
        time: initTime 
      }}
      onFinish={onSave}
      onValuesChange={handleValuesChange}
    >
      <Form.Item label="Enable" name="enabled" valuePropName="checked">
        <Switch />
      </Form.Item>

      <Form.Item
        label="Date (Weekday)"
        name="day_of_week"
        rules={[{ required: true, message: 'Please choose date' }]}
      >
        <Select options={DOW_OPTIONS} />
      </Form.Item>

      <Form.Item
        label={`Time（${initial.day_of_week}）`}
        name="time"
        rules={[{ required: true, message: 'Please choose time' }]}
      >
        <TimePicker format="HH:mm" minuteStep={5} />
      </Form.Item>

      <Space>
        <Button type="primary" htmlType="submit" loading={saving} disabled={!isDirty}>
          Save
        </Button>
        <Typography.Text type="secondary">
          Runs every two weeks (Cron), Time Zone {initial.timezone || 'Australia/Sydney'}
        </Typography.Text>
      </Space>
    </Form>
  );
}
