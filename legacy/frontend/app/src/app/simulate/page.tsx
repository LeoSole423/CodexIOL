import type { Metadata } from "next";
import { SimulateView } from "@/features/simulate/SimulateView";

export const metadata: Metadata = { title: "Simulación | IOL Dashboard" };

export default function SimulatePage() {
  return <SimulateView />;
}
