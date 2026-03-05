"use client";

import { useQuery } from "@tanstack/react-query";
import { fetchMarketsOverview } from "../api-client";

export function useMarketsOverview() {
  return useQuery({
    queryKey: ["markets-overview"],
    queryFn: fetchMarketsOverview,
  });
}
