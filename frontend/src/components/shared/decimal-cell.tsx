import { cn } from "@/lib/utils";
import { financialColor } from "@/lib/formatters";

interface DecimalCellProps {
  value: string | null | undefined;
  formatter: (v: string | null | undefined) => string;
  colored?: boolean;
}

export function DecimalCell({ value, formatter, colored }: DecimalCellProps) {
  const colorClass = colored ? financialColor(value) : "text-foreground";
  return (
    <span className={cn("tabular-nums", colorClass)}>
      {formatter(value)}
    </span>
  );
}
