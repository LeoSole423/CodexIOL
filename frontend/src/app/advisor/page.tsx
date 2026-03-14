import type { Metadata } from "next";
import { AdvisorView } from "@/features/advisor/AdvisorView";

export const metadata: Metadata = { title: "Asesor | IOL Dashboard" };

export default function AdvisorPage() {
  return <AdvisorView />;
}
