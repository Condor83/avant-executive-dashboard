"use client";

import { useQuery } from "@tanstack/react-query";
import { fetchPositions } from "../api-client";
import type { PositionFilters } from "../types";

export function usePositions(filters: PositionFilters = {}) {
  return useQuery({
    queryKey: ["positions", filters],
    queryFn: () => fetchPositions(filters),
  });
}
