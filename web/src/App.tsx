import { Route, Routes } from "react-router";
import { Layout } from "./components/Layout";
import Aterfynd from "./pages/Aterfynd";
import Fenologi from "./pages/Fenologi";
import Hem from "./pages/Hem";
import Morfometri from "./pages/Morfometri";
import NotFound from "./pages/NotFound";
import Om from "./pages/Om";
import Sammanfattning from "./pages/sammanfattning/Sammanfattning";
import Trender from "./pages/trender/Trender";
import Vader from "./pages/Vader";

export function AppRoutes() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<Hem />} />
        <Route path="sammanfattning" element={<Sammanfattning />} />
        <Route path="trender" element={<Trender />} />
        <Route path="morfometri" element={<Morfometri />} />
        <Route path="fenologi" element={<Fenologi />} />
        <Route path="aterfynd" element={<Aterfynd />} />
        <Route path="vader" element={<Vader />} />
        <Route path="om" element={<Om />} />
        <Route path="*" element={<NotFound />} />
      </Route>
    </Routes>
  );
}
