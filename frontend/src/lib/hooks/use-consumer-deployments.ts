"use client";

import { useQuery } from "@tanstack/react-query";
import { fetchConsumerDeployments } from "../api-client";

export function useConsumerDeployments(
  product: "all" | "avusd" | "aveth" | "avbtc" = "all",
) {
  return useQuery({
    queryKey: ["consumer-deployments", product],
    queryFn: () => fetchConsumerDeployments(product),
  });
}
