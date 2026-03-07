"use client";

import { useQuery } from "@tanstack/react-query";
import { fetchUiMetadata } from "../api-client";

export function useUiMetadata() {
  return useQuery({
    queryKey: ["ui-metadata"],
    queryFn: fetchUiMetadata,
    staleTime: 60_000,
  });
}
