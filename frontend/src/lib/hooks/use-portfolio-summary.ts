"use client";

import { useQuery } from "@tanstack/react-query";
import { fetchPortfolioSummary } from "../api-client";

export function usePortfolioSummary() {
  return useQuery({
    queryKey: ["portfolio-summary"],
    queryFn: fetchPortfolioSummary,
  });
}
