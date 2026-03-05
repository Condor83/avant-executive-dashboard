import { Badge } from "@/components/ui/badge";
import { STATUS_COLORS } from "@/lib/constants";
import { cn } from "@/lib/utils";

export function StatusBadge({ status }: { status: string }) {
  const colors = STATUS_COLORS[status.toLowerCase()] ?? {
    bg: "bg-slate-100",
    text: "text-slate-700",
  };
  return (
    <Badge
      variant="secondary"
      className={cn("text-xs font-medium capitalize", colors.bg, colors.text)}
    >
      {status}
    </Badge>
  );
}
