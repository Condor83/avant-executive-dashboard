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
    bg: "bg-slate-100",
    text: "text-slate-700",
  };
  return (
    <Badge
      variant="secondary"
      className={cn("text-xs font-medium", colors.bg, colors.text)}
    >
      {label ?? severity}
    </Badge>
  );
}
