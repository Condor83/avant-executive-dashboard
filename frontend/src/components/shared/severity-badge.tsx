import { Badge } from "@/components/ui/badge";
import { SEVERITY_COLORS } from "@/lib/constants";
import { cn } from "@/lib/utils";

export function SeverityBadge({
  severity,
  label,
}: {
  severity: string;
  label?: string;
}) {
  const colors = SEVERITY_COLORS[severity.toLowerCase()] ?? {
    bg: "bg-transparent",
    text: "text-muted-foreground",
    border: "border-border",
  };
  return (
    <div
      className={cn(
        "inline-flex items-center rounded-sm border px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider transition-colors",
        colors.bg,
        colors.text,
        colors.border
      )}
    >
      {label ?? severity}
    </div>
  );
}
