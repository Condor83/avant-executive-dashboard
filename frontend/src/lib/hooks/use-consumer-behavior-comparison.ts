"use client";

import { useQuery } from "@tanstack/react-query";
import { fetchConsumerBehaviorComparison } from "../api-client";

export function useConsumerBehaviorComparison(
  product: "all" | "avusd" | "aveth" | "avbtc" = "all",
) {
  return useQuery({
    queryKey: ["consumer-behavior-comparison", product],
    queryFn: () => fetchConsumerBehaviorComparison(product),
  });
}
