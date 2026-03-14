import type { Metadata } from "next";
import { AssetsView } from "@/features/assets/AssetsView";

export const metadata: Metadata = { title: "Activos | IOL Dashboard" };

export default function AssetsPage() {
  return <AssetsView />;
}
