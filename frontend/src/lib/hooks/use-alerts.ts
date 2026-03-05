"use client";

import { useQuery } from "@tanstack/react-query";
import { fetchAlerts } from "../api-client";
import type { AlertFilters } from "../types";

export function useAlerts(filters: AlertFilters = {}) {
  return useQuery({
    queryKey: ["alerts", filters],
    queryFn: () => fetchAlerts(filters),
  });
}
