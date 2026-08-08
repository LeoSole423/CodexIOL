import type { Metadata } from "next";
import { HistoryView } from "@/features/history/HistoryView";

export const metadata: Metadata = { title: "Historia | IOL Dashboard" };

export default function HistoryPage() {
  return <HistoryView />;
}
