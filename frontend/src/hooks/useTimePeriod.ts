import { useCallback, useSyncExternalStore } from "react";
import { getTimePeriod } from "@/lib/time-of-day";
import type { TimePeriod } from "@/lib/types";

/** Returns the current time period for a city, updating every 60 seconds. */
export function useTimePeriod(city: string): TimePeriod {
  const subscribe = useCallback(
    (cb: () => void) => {
      const id = setInterval(cb, 60_000);
      return () => clearInterval(id);
    },
    [],
  );

  const getSnapshot = useCallback(() => getTimePeriod(city), [city]);

  return useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
}
