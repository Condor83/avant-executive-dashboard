"use client";

import { useQuery } from "@tanstack/react-query";
import { fetchDataQuality } from "../api-client";

export function useDataQuality() {
  return useQuery({
    queryKey: ["data-quality"],
    queryFn: fetchDataQuality,
  });
}
