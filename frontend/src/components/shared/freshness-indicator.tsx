import { cn } from "@/lib/utils";
import { FRESHNESS_THRESHOLDS } from "@/lib/constants";
import { formatAge } from "@/lib/formatters";

export function FreshnessIndicator({ hours }: { hours: number | null }) {
  if (hours === null) {
    return (
      <span className="inline-flex items-center gap-1.5 text-xs text-muted-foreground">
        <span className="h-2 w-2 rounded-full bg-slate-300" />
        N/A
      </span>
    );
  }

  let color = "bg-emerald-500";
  if (hours >= FRESHNESS_THRESHOLDS.warn) color = "bg-red-500";
  else if (hours >= FRESHNESS_THRESHOLDS.good) color = "bg-amber-500";

  return (
    <span className="inline-flex items-center gap-1.5 text-xs text-foreground">
      <span className={cn("h-2 w-2 rounded-full", color)} />
      {formatAge(hours)}
    </span>
  );
}
