import type { Metadata } from "next";
import { QualityView } from "@/features/quality/QualityView";

export const metadata: Metadata = { title: "Calidad | IOL Dashboard" };

export default function QualityPage() {
  return <QualityView />;
}
