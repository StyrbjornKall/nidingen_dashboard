import { Navigate, Route, Routes, useLocation } from "react-router";
import { Layout } from "./components/Layout";
import Aterfynd from "./pages/aterfynd/Aterfynd";
import Fenologi from "./pages/fenologi/Fenologi";
import Hem from "./pages/hem/Hem";
import Biometri from "./pages/biometri/Biometri";
import NotFound from "./pages/NotFound";
import Om from "./pages/om/Om";
import Sammanfattning from "./pages/sammanfattning/Sammanfattning";
import Trender from "./pages/trender/Trender";
import Vader from "./pages/vader/Vader";

/** The page was called Morfometri before; keep old links (and their ?arter=) working. */
function OldBiometriRedirect() {
  const { search } = useLocation();
  return <Navigate to={{ pathname: "/biometri", search }} replace />;
}

export function AppRoutes() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<Hem />} />
        <Route path="sammanfattning" element={<Sammanfattning />} />
        <Route path="trender" element={<Trender />} />
        <Route path="biometri" element={<Biometri />} />
        <Route path="morfometri" element={<OldBiometriRedirect />} />
        <Route path="fenologi" element={<Fenologi />} />
        <Route path="aterfynd" element={<Aterfynd />} />
        <Route path="vader" element={<Vader />} />
        <Route path="om" element={<Om />} />
        <Route path="*" element={<NotFound />} />
      </Route>
    </Routes>
  );
}
