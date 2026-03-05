"use client";

import { useQuery } from "@tanstack/react-query";
import { fetchWatchlist } from "../api-client";

export function useWatchlist() {
  return useQuery({
    queryKey: ["watchlist"],
    queryFn: fetchWatchlist,
  });
}
