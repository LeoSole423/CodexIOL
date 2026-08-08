import { cn } from "@/lib/utils";
import { fmtPct, fmtARS } from "@/lib/formatters";

interface DeltaBadgeProps {
  value: number | null | undefined;
  format?: "pct" | "ars";
  size?: "sm" | "md";
  className?: string;
}

export function DeltaBadge({
  value,
  format = "pct",
  size = "sm",
  className,
}: DeltaBadgeProps) {
  if (value == null) {
    return (
      <span
        className={cn(
          "inline-flex items-center rounded-md px-1.5 py-0.5 font-medium text-muted-foreground bg-muted/20",
          size === "sm" ? "text-xs" : "text-sm",
          className
        )}
      >
        —
      </span>
    );
  }

  const isPositive = value > 0;
  const isNegative = value < 0;

  const colorClass = isPositive
    ? "bg-success/10 text-success"
    : isNegative
      ? "bg-danger/10 text-danger"
      : "bg-muted/20 text-muted-foreground";

  const formatted =
    format === "pct"
      ? fmtPct(value)
      : (value > 0 ? "+" : "") + fmtARS(value);

  return (
    <span
      className={cn(
        "inline-flex items-center rounded-md px-1.5 py-0.5 font-medium",
        size === "sm" ? "text-xs" : "text-sm",
        colorClass,
        className
      )}
    >
      {formatted}
    </span>
  );
}
