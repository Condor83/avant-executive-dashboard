"use client";

import { useQuery } from "@tanstack/react-query";
import { fetchMarketHistory } from "../api-client";

export function useMarketHistory(marketId: number, days: number = 30) {
  return useQuery({
    queryKey: ["market-history", marketId, days],
    queryFn: () => fetchMarketHistory(marketId, days),
    enabled: marketId > 0,
  });
}
