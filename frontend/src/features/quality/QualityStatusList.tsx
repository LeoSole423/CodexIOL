"use client";

import { useState } from "react";
import { ChevronRight } from "lucide-react";
import { StatusBadge } from "@/components/ui/StatusBadge";
import { EmptyState } from "@/components/ui/EmptyState";
import { cn } from "@/lib/utils";
import type { QualityRow } from "@/types/quality";

interface QualityStatusListProps {
  rows: QualityRow[];
  onSelect: (id: string) => void;
  selectedId: string | null;
}

export function QualityStatusList({ rows, onSelect, selectedId }: QualityStatusListProps) {
  if (rows.length === 0) {
    return <EmptyState message="Sin datos de calidad" />;
  }

  return (
    <div className="divide-y divide-border rounded-lg border border-border overflow-hidden">
      {rows.map((row) => (
        <button
          key={row.id}
          onClick={() => onSelect(row.id === selectedId ? "" : row.id)}
          className={cn(
            "w-full flex items-center gap-3 px-4 py-3 text-left transition-colors",
            "hover:bg-muted/10",
            selectedId === row.id && "bg-muted/15"
          )}
        >
          <StatusBadge status={row.kind} showIcon label={undefined} />
          <div className="flex-1 min-w-0">
            <p className="text-sm font-medium text-foreground">{row.label}</p>
            <p className="text-xs text-muted-foreground truncate">{row.value}</p>
          </div>
          <ChevronRight
            className={cn(
              "h-4 w-4 text-muted-foreground/50 shrink-0 transition-transform duration-150",
              selectedId === row.id && "rotate-90"
            )}
          />
        </button>
      ))}
    </div>
  );
}
