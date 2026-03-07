"use client";

import { useQuery } from "@tanstack/react-query";
import { fetchMarketExposures } from "../api-client";
import type { MarketExposureFilters } from "../types";

export function useMarketExposures(filters: MarketExposureFilters = {}) {
  return useQuery({
    queryKey: ["market-exposures", filters],
    queryFn: () => fetchMarketExposures(filters),
  });
}
