

import React from "react";
import { Navigate } from "react-router-dom";
import { me } from "../api/auth";

export function ProtectedRoute({ children }: { children: React.ReactNode }) {
  const [loading, setLoading] = React.useState(true);
  const [ok, setOk] = React.useState(false);

  React.useEffect(() => {
    let mounted = true;
    me()
      .then(() => mounted && setOk(true))
      .catch(() => mounted && setOk(false))
      .finally(() => mounted && setLoading(false));
    return () => {
      mounted = false;
    };
  }, []);

  if (loading) return null; // 也可以放一个全局 Spin

  if (!ok) return <Navigate to="/login" replace />;

  return <>{children}</>;
}
