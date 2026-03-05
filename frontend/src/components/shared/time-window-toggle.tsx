"use client";

import { Button } from "@/components/ui/button";
import { TIME_WINDOWS, type TimeWindow } from "@/lib/constants";
import { cn } from "@/lib/utils";

const LABELS: Record<TimeWindow, string> = {
  yesterday: "Yesterday",
  "7d": "7D",
  "30d": "30D",
};

interface TimeWindowToggleProps {
  value: TimeWindow;
  onChange: (w: TimeWindow) => void;
}

export function TimeWindowToggle({ value, onChange }: TimeWindowToggleProps) {
  return (
    <div className="inline-flex gap-1 rounded-lg border border-slate-200 bg-white p-0.5">
      {TIME_WINDOWS.map((w) => (
        <Button
          key={w}
          variant="ghost"
          size="sm"
          className={cn(
            "h-7 rounded-md px-3 text-xs font-medium",
            value === w
              ? "bg-blue-50 text-blue-700"
              : "text-slate-600 hover:text-slate-900",
          )}
          onClick={() => onChange(w)}
        >
          {LABELS[w]}
        </Button>
      ))}
    </div>
  );
}
