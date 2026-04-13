import { create } from "zustand";
import type { City, TempUnit } from "@/lib/types";

interface CityState {
  city: City;
  setCity: (city: City) => void;
  toggle: () => void;
  tempUnit: TempUnit;
  setTempUnit: (unit: TempUnit) => void;
  toggleTempUnit: () => void;
}

export const useCityStore = create<CityState>((set) => ({
  city: "nyc",
  setCity: (city) => set({ city }),
  toggle: () => set((s) => ({ city: s.city === "nyc" ? "london" : "nyc" })),
  tempUnit: "fahrenheit",
  setTempUnit: (tempUnit) => set({ tempUnit }),
  toggleTempUnit: () =>
    set((s) => ({ tempUnit: s.tempUnit === "fahrenheit" ? "celsius" : "fahrenheit" })),
}));
