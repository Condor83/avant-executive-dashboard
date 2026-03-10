"use client";

import { useQuery } from "@tanstack/react-query";
import { fetchConsumerTopWallets } from "../api-client";
import type { ConsumerWalletRankMode } from "../types";

export function useConsumerTopWallets(
  product: "all" | "avusd" | "aveth" | "avbtc" = "all",
  rank: ConsumerWalletRankMode = "assets",
  limit: number = 25,
) {
  return useQuery({
    queryKey: ["consumer-top-wallets", product, rank, limit],
    queryFn: () => fetchConsumerTopWallets(product, rank, limit),
  });
}
