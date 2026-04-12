"use client";

import { useState } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { useScrolledPast } from "@/hooks/useScrollPosition";
import { useCityStore } from "@/store/useCityStore";

const NAV_LINKS = [
  { href: "/analytics", label: "Analytics" },
  { href: "/weather", label: "Weather" },
  { href: "/stations", label: "Stations" },
  { href: "/compare", label: "Compare" },
];

function CityToggleInline() {
  const { city, toggle } = useCityStore();

  return (
    <button
      onClick={toggle}
      className="flex items-center gap-0.5 rounded-full px-0.5 py-0.5 transition-colors duration-300"
      style={{
        background: "rgba(255, 255, 255, 0.06)",
        border: "1px solid rgba(255,255,255,0.1)",
      }}
      aria-label={`Switch to ${city === "nyc" ? "London" : "NYC"}`}
    >
      <span
        className="px-3 py-1 rounded-full text-xs font-medium transition-all duration-300"
        style={{
          background: city === "nyc" ? "rgba(255,255,255,0.15)" : "transparent",
          color: city === "nyc" ? "rgba(255,255,255,0.95)" : "rgba(255,255,255,0.5)",
        }}
      >
        NYC
      </span>
      <span
        className="px-3 py-1 rounded-full text-xs font-medium transition-all duration-300"
        style={{
          background: city === "london" ? "rgba(255,255,255,0.15)" : "transparent",
          color: city === "london" ? "rgba(255,255,255,0.95)" : "rgba(255,255,255,0.5)",
        }}
      >
        London
      </span>
    </button>
  );
}

export default function NavBar() {
  const scrolled = useScrolledPast(50);
  const pathname = usePathname();
  const [menuOpen, setMenuOpen] = useState(false);
  const isLanding = pathname === "/";
  const showGlass = !isLanding || scrolled;

  return (
    <nav className="fixed top-0 left-0 right-0 z-40">
      <div
        className="mx-auto max-w-5xl mt-3 px-6 py-3 rounded-full flex items-center justify-between transition-all duration-500"
        style={{
          background: showGlass ? "rgba(15, 18, 25, 0.8)" : "rgba(15, 18, 25, 0)",
          border: showGlass ? "1px solid rgba(255,255,255,0.06)" : "1px solid transparent",
          backdropFilter: showGlass ? "blur(16px)" : "none",
        }}
      >
        <Link
          href="/"
          className="text-sm font-medium text-white/80 tracking-wide hover:text-white transition-colors whitespace-nowrap"
        >
          City Cycles
        </Link>

        <div className="hidden sm:flex">
          <CityToggleInline />
        </div>

        <div className="hidden sm:flex items-center gap-4">
          {NAV_LINKS.map((link) => (
            <Link
              key={link.href}
              href={link.href}
              className="text-xs tracking-wide transition-colors"
              style={{
                color:
                  pathname === link.href
                    ? "rgba(255,255,255,0.9)"
                    : "rgba(255,255,255,0.5)",
              }}
            >
              {link.label}
            </Link>
          ))}
        </div>

        <button
          className="sm:hidden flex flex-col gap-1 p-1"
          onClick={() => setMenuOpen(!menuOpen)}
          aria-label="Toggle menu"
        >
          <span
            className="block w-4 h-0.5 bg-white/70 transition-transform duration-300"
            style={{
              transform: menuOpen ? "rotate(45deg) translate(2px, 2px)" : "none",
            }}
          />
          <span
            className="block w-4 h-0.5 bg-white/70 transition-opacity duration-300"
            style={{ opacity: menuOpen ? 0 : 1 }}
          />
          <span
            className="block w-4 h-0.5 bg-white/70 transition-transform duration-300"
            style={{
              transform: menuOpen ? "rotate(-45deg) translate(2px, -2px)" : "none",
            }}
          />
        </button>
      </div>

      <div
        className="sm:hidden max-w-5xl mt-1 mx-4 rounded-2xl overflow-hidden transition-all duration-300"
        style={{
          maxHeight: menuOpen ? "300px" : "0",
          opacity: menuOpen ? 1 : 0,
          background: "rgba(15, 18, 25, 0.9)",
          border: menuOpen ? "1px solid rgba(255,255,255,0.06)" : "1px solid transparent",
          backdropFilter: "blur(16px)",
        }}
      >
        <div className="p-4 flex flex-col gap-3">
          <div className="flex justify-center pb-2 border-b border-white/5">
            <CityToggleInline />
          </div>

          {NAV_LINKS.map((link) => (
            <Link
              key={link.href}
              href={link.href}
              onClick={() => setMenuOpen(false)}
              className="text-sm tracking-wide transition-colors py-1"
              style={{
                color:
                  pathname === link.href
                    ? "rgba(255,255,255,0.9)"
                    : "rgba(255,255,255,0.5)",
              }}
            >
              {link.label}
            </Link>
          ))}
        </div>
      </div>
    </nav>
  );
}
