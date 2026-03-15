import type { Metadata } from "next";
import { EnginesView } from "@/features/engines/EnginesView";

export const metadata: Metadata = { title: "Motores | IOL Dashboard" };

export default function EnginesPage() {
  return <EnginesView />;
}
