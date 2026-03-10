"use client";

import { useQuery } from "@tanstack/react-query";
import { fetchConsumerAdoptionFunnel } from "../api-client";

export function useConsumerAdoptionFunnel(
  product: "all" | "avusd" | "aveth" | "avbtc" = "all",
) {
  return useQuery({
    queryKey: ["consumer-adoption-funnel", product],
    queryFn: () => fetchConsumerAdoptionFunnel(product),
  });
}
