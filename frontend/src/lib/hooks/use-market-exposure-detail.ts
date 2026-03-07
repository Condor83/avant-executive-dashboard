"use client";

import { useQuery } from "@tanstack/react-query";
import { fetchMarketExposureDetail } from "../api-client";

export function useMarketExposureDetail(exposureSlug: string, days: number = 30) {
  return useQuery({
    queryKey: ["market-exposure-detail", exposureSlug, days],
    queryFn: () => fetchMarketExposureDetail(exposureSlug, days),
    enabled: exposureSlug.length > 0,
  });
}
