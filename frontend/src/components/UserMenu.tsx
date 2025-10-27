import { Avatar, Dropdown, Typography, message } from "antd";
import type { MenuProps } from "antd";
import { LogoutOutlined, UserOutlined } from "@ant-design/icons";
import { me, logout, type User } from "../api/auth";
import { useQuery } from "@tanstack/react-query";

const { Text } = Typography;

function initials(name?: string | null, username?: string) {
  const s = (name || username || "").trim();
  if (!s) return "U";
  const parts = s.split(/\s+/);
  return (parts[0][0] + (parts[1]?.[0] ?? "")).toUpperCase();
}

export default function UserMenu() {
  const { data: user } = useQuery<User>({
    queryKey: ["auth", "me"],
    queryFn: me,
    staleTime: 60_000,
    retry: false,
  });

  const items: MenuProps["items"] = [
    {
      key: "profile",
      disabled: true,
      label: (
        <div style={{ lineHeight: 1.2 }}>
          <Text strong>{user?.full_name || user?.username || "User"}</Text>
          <div style={{ fontSize: 12, opacity: 0.7 }}>Signed in</div>
        </div>
      ),
    },
    { type: "divider" },
    {
      key: "signout",
      icon: <LogoutOutlined />,
      label: "Sign out",
      onClick: async () => {
        try {
          await logout();
        } catch {
          message.warning("Signed out locally");
        } finally {
          window.location.href = "/login";
        }
      },
    },
  ];

  return (
    <Dropdown menu={{ items }} trigger={["click"]}>
      <div style={{ display: "flex", alignItems: "center", cursor: "pointer", gap: 8 }}>
        <Avatar size="large" icon={<UserOutlined />}>
          {initials(user?.full_name, user?.username)}
        </Avatar>
      </div>
    </Dropdown>
  );
}
