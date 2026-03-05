"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  LayoutDashboard,
  Briefcase,
  TrendingUp,
  ShieldAlert,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { useDataQuality } from "@/lib/hooks/use-data-quality";
import { FRESHNESS_THRESHOLDS } from "@/lib/constants";

const NAV = [
  { label: "Summary", href: "/", icon: LayoutDashboard },
  { label: "Portfolio", href: "/portfolio", icon: Briefcase },
  { label: "Markets", href: "/markets", icon: TrendingUp },
  { label: "Risk", href: "/risk", icon: ShieldAlert },
] as const;

function DqHealthDot() {
  const { data } = useDataQuality();
  if (!data) return null;

  const posAge = data.freshness.position_snapshot_age_hours;
  const mktAge = data.freshness.market_snapshot_age_hours;
  const worstAge = Math.max(posAge ?? Infinity, mktAge ?? Infinity);

  let color = "bg-emerald-500";
  if (worstAge >= FRESHNESS_THRESHOLDS.warn) color = "bg-red-500";
  else if (worstAge >= FRESHNESS_THRESHOLDS.good) color = "bg-amber-500";

  return (
    <div className="flex items-center gap-2 text-xs text-slate-500">
      <span className={cn("h-2 w-2 rounded-full", color)} />
      Data: {worstAge === Infinity ? "N/A" : `${worstAge.toFixed(1)}h`}
    </div>
  );
}

export function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className="fixed inset-y-0 left-0 z-30 flex w-56 flex-col border-r border-slate-200 bg-white">
      <div className="flex h-14 items-center gap-2 border-b border-slate-200 px-4">
        <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-[var(--avant-navy)] text-xs font-bold text-white">
          A
        </div>
        <span className="text-lg font-semibold text-slate-900">Avant</span>
      </div>

      <nav className="flex flex-1 flex-col gap-1 px-3 py-4">
        {NAV.map(({ label, href, icon: Icon }) => {
          const active = href === "/" ? pathname === "/" : pathname.startsWith(href);
          return (
            <Link
              key={href}
              href={href}
              className={cn(
                "flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-colors",
                active
                  ? "bg-blue-50 text-blue-700"
                  : "text-slate-600 hover:bg-slate-50 hover:text-slate-900",
              )}
            >
              <Icon className="h-4 w-4" />
              {label}
            </Link>
          );
        })}
      </nav>

      <div className="border-t border-slate-200 px-4 py-3">
        <DqHealthDot />
      </div>
    </aside>
  );
}
