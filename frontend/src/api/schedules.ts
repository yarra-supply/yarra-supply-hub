
import { http } from '@/api/http';
import type { Schedule, ScheduleKey } from '@/types/schedule';

// 查询定时任务接口
export async function getSchedules(): Promise<Schedule[]> {
    //GET /api/v1/schedules → Schedule[]（最多两条）
    const { data } = await http.get('/schedules');
    return data;
}


// 更新
export async function updateSchedule(
    key: ScheduleKey, payload: Schedule
): Promise<Schedule> {
    // 后端不需要 label/updated_at，key 已包含在路径
    const {
        label,
        key: _key,
        updated_at: _updatedAt,
        ...rest
    } = payload as any;

    const body = {
        ...rest,
        timezone: rest.timezone || 'Australia/Sydney',
    };

    const { data } = await http.put(`/schedules/${key}`, body);
    return data;
}
