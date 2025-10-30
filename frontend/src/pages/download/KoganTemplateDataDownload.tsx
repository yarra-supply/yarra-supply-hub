
import React, { useState } from "react";
import { Button, Space, Typography, message } from "antd";
import { CloudDownloadOutlined } from "@ant-design/icons";
import {
  applyKoganTemplateExport,
  downloadKoganTemplateCSV,
  downloadKoganTemplateCSVByJob,
} from "@/api/download";
import type {
  CountryType,
  KoganExportJobSummary,
} from "@/types/koganTemplateDownload";


const { Title, Text } = Typography;



const KoganTemplateDataDownload: React.FC = () => {
  const [jobs, setJobs] = useState<Record<CountryType, KoganExportJobSummary | undefined>>({
    AU: undefined,
    NZ: undefined,
  });

  const [loadingMap, setLoadingMap] = useState<Record<CountryType, {
    download: boolean;
    redownload: boolean;
    apply: boolean;
  }>>({
    AU: { download: false, redownload: false, apply: false },
    NZ: { download: false, redownload: false, apply: false },
  });

  const setLoading = (
    country: CountryType,
    key: "download" | "redownload" | "apply",
    value: boolean,
  ) => {
    setLoadingMap((prev) => ({
      ...prev,
      [country]: { ...prev[country], [key]: value },
    }));
  };

  const handleDownload = async (country: CountryType) => {
    setLoading(country, "download", true);
    try {
      const job = await downloadKoganTemplateCSV(country);
      setJobs((prev) => ({ ...prev, [country]: job }));
      message.success(
        `Kogan ${country} CSV 已生成并下载（行数：${job.row_count}）。`,
      );
    } catch (err: any) {
      console.error(err);
      message.error(err?.message || `Kogan ${country} CSV download failed!`);
    } finally {
      setLoading(country, "download", false);
    }
  };

  const handleRedownload = async (country: CountryType) => {
    const job = jobs[country];
    if (!job) {
      message.warning(`请先生成 ${country} 的导出任务`);
      return;
    }
    setLoading(country, "redownload", true);
    try {
      await downloadKoganTemplateCSVByJob(job.job_id, job.file_name);
      message.success(`已重新下载 Kogan ${country} CSV`);
    } catch (err: any) {
      console.error(err);
      message.error(err?.message || `Kogan ${country} CSV re-download failed!`);
    } finally {
      setLoading(country, "redownload", false);
    }
  };

  const handleApply = async (country: CountryType) => {
    const job = jobs[country];
    if (!job) {
      message.warning(`请先生成 ${country} 的导出任务`);
      return;
    }
    setLoading(country, "apply", true);
    try {
      const res = await applyKoganTemplateExport(job.job_id);
      message.success(
        `已确认 ${country} 导出并回写成功${res.applied_at ? `（${res.applied_at}）` : ""}`,
      );
      setJobs((prev) => ({ ...prev, [country]: undefined }));
    } catch (err: any) {
      console.error(err);
      message.error(err?.message || `确认 ${country} 导出失败`);
    } finally {
      setLoading(country, "apply", false);
    }
  };

  const renderJobInfo = (country: CountryType) => {
    const job = jobs[country];
    if (!job) return null;

    return (
      <div style={{ marginTop: 8 }}>
        <Text type="secondary">
          最近一次导出：job_id {job.job_id}（行数: {job.row_count}）
        </Text>
        <Space style={{ marginTop: 8 }}>
          <Button
            size="small"
            onClick={() => handleRedownload(country)}
            loading={loadingMap[country].redownload}
          >
            重新下载
          </Button>
          <Button
            size="small"
            type="default"
            onClick={() => handleApply(country)}
            loading={loadingMap[country].apply}
          >
            标记导出完成
          </Button>
        </Space>
      </div>
    );
  };

  return (
    <div style={{ padding: 16 }}>
      <Title level={4} style={{ marginBottom: 12 }}>
        Kogan Template Download
      </Title>
      <Text type="secondary">
        only the <strong> export data needs to be changed. Fill in only updated value,</strong> leave the other columns blank
      </Text>

      <div style={{ marginTop: 16 }}>
        <Space size="middle" wrap>
          <Button
            type="primary"
            icon={<CloudDownloadOutlined />}
            loading={loadingMap.AU.download}
            onClick={() => handleDownload("AU")}
          >
            AU Kogan Download
          </Button>

          <Button
            type="primary"
            icon={<CloudDownloadOutlined />}
            loading={loadingMap.NZ.download}
            onClick={() => handleDownload("NZ")}
          >
            NZ Kogan Download
          </Button>
        </Space>
      </div>

      {renderJobInfo("AU")}
      {renderJobInfo("NZ")}
    </div>
  );
};

export default KoganTemplateDataDownload;
