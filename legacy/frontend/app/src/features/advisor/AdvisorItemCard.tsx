"use client";

import { useState } from "react";
import { ChevronDown, ChevronUp } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { humanize } from "@/lib/formatters";
import type { AdvisorItem } from "@/types/advisor";

const ACTION_VARIANTS: Record<string, "default" | "success" | "danger" | "warning" | "muted"> = {
  buy: "success",
  comprar: "success",
  sell: "danger",
  vender: "danger",
  review: "warning",
  revisar: "warning",
  watch: "muted",
  hold: "muted",
};

function actionVariant(action?: string): "default" | "success" | "danger" | "warning" | "muted" {
  if (!action) return "muted";
  const key = action.toLowerCase();
  return ACTION_VARIANTS[key] ?? "muted";
}

interface AdvisorItemCardProps {
  item: AdvisorItem;
}

export function AdvisorItemCard({ item }: AdvisorItemCardProps) {
  const [expanded, setExpanded] = useState(false);

  return (
    <div className="rounded-lg border border-border bg-surface p-4 space-y-2">
      {/* Header */}
      <div className="flex items-start gap-3">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            {item.symbol && (
              <span className="text-sm font-semibold font-mono text-foreground">
                {item.symbol as string}
              </span>
            )}
            {item.action && (
              <Badge variant={actionVariant(item.action as string)}>
                {humanize(item.action as string)}
              </Badge>
            )}
            {item.confidence && (
              <span className="text-xs text-muted-foreground">
                {humanize(item.confidence as string)}
              </span>
            )}
            {item.status && (
              <span className="text-xs text-muted-foreground">· {humanize(item.status as string)}</span>
            )}
          </div>
          {item.reason && (
            <p className="text-xs text-muted-foreground mt-1 line-clamp-2">{item.reason as string}</p>
          )}
        </div>

        {item.analysis && (
          <Button
            variant="ghost"
            size="icon"
            className="h-7 w-7 text-muted-foreground shrink-0"
            onClick={() => setExpanded((e) => !e)}
          >
            {expanded ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
          </Button>
        )}
      </div>

      {/* Quality codes */}
      {Array.isArray(item.quality_codes) && item.quality_codes.length > 0 && (
        <div className="flex flex-wrap gap-1">
          {(item.quality_codes as string[]).map((code) => (
            <span
              key={code}
              className="text-xs bg-muted/20 text-muted-foreground rounded-md px-1.5 py-0.5 font-mono"
            >
              {code}
            </span>
          ))}
        </div>
      )}

      {/* Expanded analysis */}
      {expanded && item.analysis && (
        <div className="pt-2 border-t border-border">
          <p className="text-xs text-foreground whitespace-pre-line leading-relaxed">
            {item.analysis as string}
          </p>
        </div>
      )}
    </div>
  );
}
