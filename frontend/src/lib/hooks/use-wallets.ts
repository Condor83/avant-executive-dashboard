"use client";

import { useQuery } from "@tanstack/react-query";
import { fetchWallets } from "../api-client";

export function useWallets() {
  return useQuery({
    queryKey: ["wallets"],
    queryFn: fetchWallets,
  });
}
