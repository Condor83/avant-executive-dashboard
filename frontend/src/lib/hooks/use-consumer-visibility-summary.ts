"use client";

import { useQuery } from "@tanstack/react-query";
import { fetchConsumerVisibilitySummary } from "../api-client";

export function useConsumerVisibilitySummary() {
  return useQuery({
    queryKey: ["consumer-visibility-summary"],
    queryFn: fetchConsumerVisibilitySummary,
  });
}
