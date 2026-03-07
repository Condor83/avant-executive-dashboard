import { Badge } from "@/components/ui/badge";
import { STATUS_COLORS } from "@/lib/constants";
import { cn } from "@/lib/utils";

export function StatusBadge({
  status,
  label,
}: {
  status: string;
  label?: string;
}) {
  const colors = STATUS_COLORS[status.toLowerCase()] ?? {
    bg: "bg-slate-100",
    text: "text-slate-700",
  };
  return (
    <Badge
      variant="secondary"
      className={cn("text-xs font-medium", colors.bg, colors.text)}
    >
      {label ?? status}
    </Badge>
  );
}
