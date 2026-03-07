"use client";

import { useQuery } from "@tanstack/react-query";
import { fetchMarketSummary } from "../api-client";

export function useMarketSummary() {
  return useQuery({
    queryKey: ["market-summary"],
    queryFn: fetchMarketSummary,
  });
}
