"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  LayoutDashboard,
  Briefcase,
  TrendingUp,
  ShieldAlert,
  Users,
  Wallet,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { useDataQuality } from "@/lib/hooks/use-data-quality";
import { FRESHNESS_THRESHOLDS } from "@/lib/constants";
import { ThemeToggle } from "@/components/shared/theme-toggle";

const NAV = [
  { label: "Summary", href: "/", icon: LayoutDashboard },
  { label: "Portfolio", href: "/portfolio", icon: Briefcase },
  { label: "Markets", href: "/markets", icon: TrendingUp },
  { label: "Holders", href: "/consumer", icon: Users },
  { label: "Wallets", href: "/wallets", icon: Wallet },
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
    <div className="flex items-center gap-2 text-xs text-muted-foreground">
      <span className={cn("h-2 w-2 rounded-full", color)} />
      Data: {worstAge === Infinity ? "N/A" : `${worstAge.toFixed(1)}h`}
    </div>
  );
}

export function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className="fixed inset-y-0 left-0 z-30 flex w-56 flex-col border-r border-border bg-card">
      <div className="flex h-14 items-center gap-2 border-b border-border px-4">
        <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-avant-navy text-xs font-bold text-white">
          A
        </div>
        <span className="text-lg font-semibold text-foreground">Avant</span>
      </div>

      <nav className="flex flex-1 flex-col gap-1 px-3 py-4">
        {NAV.map(({ label, href, icon: Icon }) => {
          const active = href === "/" ? pathname === "/" : pathname.startsWith(href);
          return (
            <Link
              key={href}
              href={href}
              className={cn(
                "flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium transition-colors",
                active
                  ? "bg-muted text-foreground"
                  : "text-muted-foreground hover:bg-muted/50 hover:text-foreground",
              )}
            >
              <Icon className="h-4 w-4" />
              {label}
            </Link>
          );
        })}
      </nav>

      <div className="flex items-center justify-between border-t border-border px-4 py-3">
        <DqHealthDot />
        <ThemeToggle />
      </div>
    </aside>
  );
}
