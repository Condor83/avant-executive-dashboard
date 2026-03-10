"use client";

import { useQuery } from "@tanstack/react-query";
import { fetchConsumerVisibilityProtocolGaps } from "../api-client";

export function useConsumerVisibilityProtocolGaps() {
  return useQuery({
    queryKey: ["consumer-visibility-protocol-gaps"],
    queryFn: fetchConsumerVisibilityProtocolGaps,
  });
}
