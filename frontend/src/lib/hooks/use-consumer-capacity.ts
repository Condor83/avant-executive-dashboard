"use client";

import { useQuery } from "@tanstack/react-query";
import { fetchConsumerCapacity } from "../api-client";

export function useConsumerCapacity() {
  return useQuery({
    queryKey: ["consumer-capacity"],
    queryFn: fetchConsumerCapacity,
  });
}
