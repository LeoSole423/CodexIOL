"use client";

import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";
import { Trash2, Plus } from "lucide-react";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import { EmptyState } from "@/components/ui/EmptyState";
import { fmtARS, fmtDate } from "@/lib/formatters";
import { useManualCashflows, useCashflowMutations } from "@/hooks/usePortfolio";

const schema = z.object({
  flow_date: z.string().min(1, "Fecha requerida"),
  kind: z.string().min(1, "Tipo requerido"),
  amount_ars: z.coerce.number({ invalid_type_error: "Monto inválido" }),
  note: z.string().optional(),
});

type FormValues = z.infer<typeof schema>;

const KIND_OPTIONS = [
  { value: "deposit", label: "Depósito" },
  { value: "withdrawal", label: "Extracción" },
  { value: "dividend", label: "Dividendo" },
  { value: "fee", label: "Comisión" },
  { value: "other", label: "Otro" },
];

export function CashflowForm() {
  const { data, isLoading } = useManualCashflows();
  const { add, remove } = useCashflowMutations();

  const {
    register,
    handleSubmit,
    setValue,
    reset,
    formState: { errors, isSubmitting },
  } = useForm<FormValues>({ resolver: zodResolver(schema) });

  async function onSubmit(values: FormValues) {
    await add.mutateAsync(values);
    reset();
  }

  return (
    <Card>
      <CardHeader className="pb-3">
        <p className="text-sm font-semibold text-foreground">Flujos manuales</p>
        <p className="text-xs text-muted-foreground">
          Correcciones manuales de caja para el cálculo de retorno real
        </p>
      </CardHeader>
      <CardContent className="pt-0 space-y-4">
        {/* Form */}
        <form onSubmit={handleSubmit(onSubmit)} className="space-y-3">
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            <div className="space-y-1">
              <Label htmlFor="flow_date" className="text-xs">Fecha</Label>
              <Input
                id="flow_date"
                type="date"
                className="text-xs h-8"
                {...register("flow_date")}
              />
              {errors.flow_date && (
                <p className="text-xs text-danger">{errors.flow_date.message}</p>
              )}
            </div>

            <div className="space-y-1">
              <Label className="text-xs">Tipo</Label>
              <Select onValueChange={(v) => setValue("kind", v)}>
                <SelectTrigger className="h-8 text-xs">
                  <SelectValue placeholder="Tipo…" />
                </SelectTrigger>
                <SelectContent>
                  {KIND_OPTIONS.map((o) => (
                    <SelectItem key={o.value} value={o.value} className="text-xs">
                      {o.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              {errors.kind && (
                <p className="text-xs text-danger">{errors.kind.message}</p>
              )}
            </div>

            <div className="space-y-1">
              <Label htmlFor="amount_ars" className="text-xs">Monto ARS</Label>
              <Input
                id="amount_ars"
                type="number"
                step="0.01"
                placeholder="0.00"
                className="text-xs h-8"
                {...register("amount_ars")}
              />
              {errors.amount_ars && (
                <p className="text-xs text-danger">{errors.amount_ars.message}</p>
              )}
            </div>

            <div className="space-y-1">
              <Label htmlFor="note" className="text-xs">Nota (opcional)</Label>
              <Input
                id="note"
                placeholder="Descripción…"
                className="text-xs h-8"
                {...register("note")}
              />
            </div>
          </div>

          <div className="flex justify-end">
            <Button type="submit" size="sm" disabled={isSubmitting || add.isPending} className="gap-1.5">
              <Plus className="h-3.5 w-3.5" />
              Agregar
            </Button>
          </div>
          {add.isError && (
            <p className="text-xs text-danger">Error al agregar. Revisá los datos.</p>
          )}
        </form>

        {/* Existing cashflows */}
        <div>
          <p className="text-xs font-medium text-muted-foreground mb-2">Ajustes existentes</p>
          {isLoading ? (
            <div className="space-y-2">
              {Array.from({ length: 3 }).map((_, i) => (
                <Skeleton key={i} className="h-9 w-full" />
              ))}
            </div>
          ) : !data?.length ? (
            <EmptyState message="Sin ajustes manuales" />
          ) : (
            <div className="divide-y divide-border rounded-md border border-border overflow-hidden">
              {data.map((cf) => (
                <div
                  key={cf.id}
                  className="flex items-center gap-3 px-3 py-2 hover:bg-muted/5"
                >
                  <span className="text-xs text-muted-foreground font-mono w-20 shrink-0">
                    {fmtDate(cf.flow_date)}
                  </span>
                  <span className="text-xs text-muted-foreground w-20 shrink-0">{cf.kind}</span>
                  <span className="text-xs font-medium text-foreground flex-1">
                    {fmtARS(cf.amount_ars)}
                  </span>
                  {cf.note && (
                    <span className="text-xs text-muted-foreground/70 truncate max-w-[120px]">
                      {cf.note}
                    </span>
                  )}
                  <Button
                    variant="ghost"
                    size="icon"
                    className="h-6 w-6 text-muted-foreground hover:text-danger shrink-0"
                    onClick={() => remove.mutate(cf.id)}
                    disabled={remove.isPending}
                  >
                    <Trash2 className="h-3.5 w-3.5" />
                  </Button>
                </div>
              ))}
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  );
}
