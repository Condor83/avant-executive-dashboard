"use client";

import { useQuery } from "@tanstack/react-query";
import { fetchWallet } from "../api-client";

export function useWallet(address: string) {
  return useQuery({
    queryKey: ["wallet", address],
    queryFn: () => fetchWallet(address),
    enabled: !!address,
  });
}
