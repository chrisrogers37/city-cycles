import { create } from "zustand";
import type { City } from "@/lib/types";

interface CityState {
  city: City;
  setCity: (city: City) => void;
  toggle: () => void;
}

export const useCityStore = create<CityState>((set) => ({
  city: "nyc",
  setCity: (city) => set({ city }),
  toggle: () => set((s) => ({ city: s.city === "nyc" ? "london" : "nyc" })),
}));
