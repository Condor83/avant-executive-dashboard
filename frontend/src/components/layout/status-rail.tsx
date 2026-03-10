"use client";

import Link from "next/link";
import { AlertTriangle } from "lucide-react";
import { FreshnessIndicator } from "@/components/shared/freshness-indicator";
import { useSummary } from "@/lib/hooks/use-summary";
import { formatDate } from "@/lib/formatters";

export function StatusRail() {
    const { data, isLoading, error } = useSummary();

    if (isLoading || error || !data) {
        return (
            <div className="flex h-10 w-full animate-pulse items-center border-b border-border bg-card px-6">
                <div className="h-3 w-32 rounded bg-muted"></div>
            </div>
        );
    }

    const { freshness } = data;

    return (
        <div className="flex h-10 w-full items-center gap-6 border-b border-border bg-card px-6 text-xs text-muted-foreground shadow-[0_1px_2px_rgba(0,0,0,0.01)]">
            <div className="flex items-center gap-2">
                <span className="font-semibold uppercase tracking-wider">Business Date</span>
                <span className="font-medium text-foreground">{formatDate(data.business_date)}</span>
            </div>

            <div className="flex items-center gap-2">
                <span className="font-semibold uppercase tracking-wider">Scope</span>
                <span className="rounded-sm bg-muted/50 px-1.5 py-0.5 font-medium text-foreground">
                    Strategy Only
                </span>
            </div>

            {freshness.open_dq_issues_24h > 0 && (
                <div className="flex items-center gap-2">
                    <span className="font-semibold uppercase tracking-wider">Data Quality</span>
                    <Link
                        href="/risk"
                        className="flex items-center gap-1.5 rounded-sm border border-avant-danger/30 bg-avant-danger/10 px-1.5 py-0.5 font-medium text-avant-danger transition-colors hover:bg-avant-danger/20"
                    >
                        <AlertTriangle className="h-3 w-3" />
                        {freshness.open_dq_issues_24h} Issue{freshness.open_dq_issues_24h !== 1 ? "s" : ""}
                    </Link>
                </div>
            )}

            <div className="flex items-center gap-2 ml-auto">
                <span className="font-semibold uppercase tracking-wider">Positions</span>
                <FreshnessIndicator hours={freshness.position_snapshot_age_hours} />
            </div>

            <div className="flex items-center gap-2">
                <span className="font-semibold uppercase tracking-wider">Markets</span>
                <FreshnessIndicator hours={freshness.market_snapshot_age_hours} />
            </div>
        </div>
    );
}
