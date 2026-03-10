"use client";

import { useQuery } from "@tanstack/react-query";
import { fetchConsumerSummary } from "../api-client";

export function useConsumerSummary(product: "all" | "avusd" | "aveth" | "avbtc" = "all") {
  return useQuery({
    queryKey: ["consumer-summary", product],
    queryFn: () => fetchConsumerSummary(product),
  });
}
