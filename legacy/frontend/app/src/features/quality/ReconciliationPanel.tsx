"use client";

import { Check, X } from "lucide-react";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { EmptyState } from "@/components/ui/EmptyState";
import { Skeleton } from "@/components/ui/skeleton";
import { useReconciliationOpen, useReconciliationMutations } from "@/hooks/useAdvisor";

export function ReconciliationPanel() {
  const { data, isLoading } = useReconciliationOpen();
  const { apply, dismiss } = useReconciliationMutations();

  const summary = data?.run?.summary;
  const rows = data?.rows ?? [];

  return (
    <Card>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <p className="text-sm font-semibold text-foreground">Reconciliación</p>
          {summary && (
            <Badge variant={summary.open_intervals > 0 ? "warning" : "success"}>
              {summary.open_intervals} pendiente{summary.open_intervals !== 1 ? "s" : ""}
            </Badge>
          )}
        </div>
        {summary?.headline && (
          <p className="text-xs text-muted-foreground">{summary.headline}</p>
        )}
      </CardHeader>
      <CardContent className="pt-0">
        {isLoading ? (
          <div className="space-y-2">
            {Array.from({ length: 3 }).map((_, i) => (
              <Skeleton key={i} className="h-12 w-full" />
            ))}
          </div>
        ) : rows.length === 0 ? (
          <EmptyState message="Sin propuestas abiertas" description="El sistema no detectó diferencias pendientes" />
        ) : (
          <div className="divide-y divide-border">
            {rows.map((proposal) => {
              const id = proposal.id as number;
              const isApplying = apply.isPending;
              const isDismissing = dismiss.isPending;
              return (
                <div key={id} className="py-3 flex items-start gap-3">
                  <div className="flex-1 min-w-0">
                    <p className="text-xs font-medium text-foreground">
                      {(proposal.resolution as string) ?? `Propuesta #${id}`}
                    </p>
                    {proposal.amount_ars != null && (
                      <p className="text-xs text-muted-foreground mt-0.5">
                        ARS {Number(proposal.amount_ars).toLocaleString("es-AR")}
                      </p>
                    )}
                    {proposal.note && (
                      <p className="text-xs text-muted-foreground/70 mt-0.5 italic">
                        {proposal.note as string}
                      </p>
                    )}
                  </div>
                  <div className="flex items-center gap-1.5 shrink-0">
                    <Button
                      size="sm"
                      variant="outline"
                      className="h-7 px-2 text-xs gap-1"
                      disabled={isApplying || isDismissing}
                      onClick={() => apply.mutate({ id })}
                    >
                      <Check className="h-3 w-3" />
                      Aplicar
                    </Button>
                    <Button
                      size="sm"
                      variant="ghost"
                      className="h-7 px-2 text-xs text-muted-foreground gap-1"
                      disabled={isApplying || isDismissing}
                      onClick={() => dismiss.mutate({ id, reason: "dismissed" })}
                    >
                      <X className="h-3 w-3" />
                    </Button>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
