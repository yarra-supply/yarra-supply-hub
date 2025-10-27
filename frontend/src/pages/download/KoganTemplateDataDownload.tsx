
import React, { useState } from "react";
import { Button, Space, Typography, message } from "antd";
import { CloudDownloadOutlined } from "@ant-design/icons";
import { downloadKoganTemplateCSV } from "@/api/download";
import type { CountryType } from "@/types/koganTemplateDownload";

const { Title, Text } = Typography;

const KoganTemplateDataDownload: React.FC = () => {
  const [loadingAU, setLoadingAU] = useState(false);
  const [loadingNZ, setLoadingNZ] = useState(false);

  const handleDownload = async (country: CountryType) => {
    const setLoading = country === "AU" ? setLoadingAU : setLoadingNZ;
    setLoading(true);
    try {
      await downloadKoganTemplateCSV(country);
      message.success(`Kogan ${country} CSV downloading, please wait`);
    } catch (err: any) {
      console.error(err);
      message.error(err?.message || `Kogan ${country} CSV download failed!`);
    } finally {
      setLoading(false);
    }
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
            loading={loadingAU}
            onClick={() => handleDownload("AU")}
          >
            AU Kogan Download
          </Button>

          <Button
            type="primary"
            icon={<CloudDownloadOutlined />}
            loading={loadingNZ}
            onClick={() => handleDownload("NZ")}
          >
            NZ Kogan Download
          </Button>
        </Space>
      </div>
    </div>
  );
};

export default KoganTemplateDataDownload;
