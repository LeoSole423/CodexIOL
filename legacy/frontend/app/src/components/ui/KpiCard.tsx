"use client";

import { TrendingDown, TrendingUp, Minus } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "./card";
import { Skeleton } from "./skeleton";
import { cn } from "@/lib/utils";

interface KpiCardProps {
  label: string;
  value: string;
  subvalue?: string;
  delta?: string | null;
  trend?: "up" | "down" | "flat" | null;
  loading?: boolean;
  className?: string;
  children?: React.ReactNode;
}

export function KpiCard({
  label,
  value,
  subvalue,
  delta,
  trend,
  loading,
  className,
  children,
}: KpiCardProps) {
  if (loading) {
    return (
      <Card className={cn("min-w-0", className)}>
        <CardHeader>
          <Skeleton className="h-3.5 w-24" />
        </CardHeader>
        <CardContent>
          <Skeleton className="h-7 w-32 mb-2" />
          <Skeleton className="h-3 w-20" />
        </CardContent>
      </Card>
    );
  }

  const TrendIcon =
    trend === "up" ? TrendingUp : trend === "down" ? TrendingDown : Minus;

  const trendClass =
    trend === "up"
      ? "text-success"
      : trend === "down"
        ? "text-danger"
        : "text-muted-foreground";

  return (
    <Card className={cn("min-w-0", className)}>
      <CardHeader>
        <CardTitle>{label}</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="text-2xl font-bold text-foreground tracking-tight truncate">
          {value}
        </div>
        {(subvalue || delta) && (
          <div className="flex items-center gap-2 mt-1.5">
            {delta && trend && (
              <span className={cn("flex items-center gap-0.5 text-xs font-medium", trendClass)}>
                <TrendIcon className="h-3 w-3" />
                {delta}
              </span>
            )}
            {subvalue && (
              <span className="text-xs text-muted-foreground truncate">{subvalue}</span>
            )}
          </div>
        )}
        {children}
      </CardContent>
    </Card>
  );
}
