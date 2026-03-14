import { CheckCircle2, AlertTriangle, XCircle, Info } from "lucide-react";
import { cn } from "@/lib/utils";
import type { QualityKind } from "@/types/quality";

interface StatusBadgeProps {
  status: QualityKind;
  label?: string;
  className?: string;
  showIcon?: boolean;
}

const STATUS_MAP: Record<
  QualityKind,
  { icon: React.ElementType; colorClass: string }
> = {
  ok: { icon: CheckCircle2, colorClass: "text-success bg-success/10" },
  warn: { icon: AlertTriangle, colorClass: "text-warning bg-warning/10" },
  danger: { icon: XCircle, colorClass: "text-danger bg-danger/10" },
  info: { icon: Info, colorClass: "text-primary bg-primary/10" },
};

export function StatusBadge({
  status,
  label,
  className,
  showIcon = true,
}: StatusBadgeProps) {
  const { icon: Icon, colorClass } = STATUS_MAP[status] ?? STATUS_MAP.info;

  return (
    <span
      className={cn(
        "inline-flex items-center gap-1 rounded-md px-1.5 py-0.5 text-xs font-medium",
        colorClass,
        className
      )}
    >
      {showIcon && <Icon className="h-3 w-3" />}
      {label}
    </span>
  );
}
