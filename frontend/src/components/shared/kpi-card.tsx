import { Card } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { cn } from "@/lib/utils";

interface KpiCardProps {
  label: string;
  value: string;
  valueClassName?: string;
  subtitle?: string;
  compact?: boolean;
}

export function KpiCard({
  label,
  value,
  valueClassName,
  subtitle,
  compact,
}: KpiCardProps) {
  return (
    <Card className="rounded-xl border border-slate-200 bg-white px-5 py-4 shadow-sm">
      <p className="text-xs font-medium uppercase tracking-wide text-slate-500">
        {label}
      </p>
      <p
        className={cn(
          "mt-1 font-bold tabular-nums",
          compact ? "text-xl" : "text-3xl",
          valueClassName ?? "text-slate-900",
        )}
      >
        {value}
      </p>
      {subtitle && (
        <p className="mt-0.5 text-xs text-slate-500">{subtitle}</p>
      )}
    </Card>
  );
}

export function KpiCardSkeleton({ compact }: { compact?: boolean }) {
  return (
    <Card className="rounded-xl border border-slate-200 bg-white px-5 py-4 shadow-sm">
      <Skeleton className="h-3 w-20" />
      <Skeleton className={cn("mt-2", compact ? "h-6 w-28" : "h-8 w-36")} />
    </Card>
  );
}
