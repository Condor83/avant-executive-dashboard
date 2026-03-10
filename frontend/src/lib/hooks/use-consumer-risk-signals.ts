"use client";

import { useQuery } from "@tanstack/react-query";
import { fetchConsumerRiskSignals } from "../api-client";

export function useConsumerRiskSignals(
  product: "all" | "avusd" | "aveth" | "avbtc" = "all",
) {
  return useQuery({
    queryKey: ["consumer-risk-signals", product],
    queryFn: () => fetchConsumerRiskSignals(product),
  });
}
