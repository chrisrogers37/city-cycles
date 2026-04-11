"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useScrollPosition } from "@/hooks/useScrollPosition";

interface NavBarProps {
  /** When true, nav is always visible (used on secondary pages). */
  alwaysVisible?: boolean;
}

/** Floating nav bar — hidden on landing until scrolled past the weather scene. */
export default function NavBar({ alwaysVisible }: NavBarProps) {
  const scrollY = useScrollPosition();
  const pathname = usePathname();
  const visible =
    alwaysVisible || scrollY > (typeof window !== "undefined" ? window.innerHeight * 0.8 : 600);

  return (
    <nav
      className="fixed top-0 left-0 right-0 z-40 transition-all duration-500"
      style={{
        opacity: visible ? 1 : 0,
        pointerEvents: visible ? "auto" : "none",
        transform: visible ? "translateY(0)" : "translateY(-100%)",
      }}
    >
      <div
        className="mx-auto max-w-5xl mt-3 px-6 py-3 rounded-full flex items-center justify-between"
        style={{
          background: "rgba(15, 18, 25, 0.8)",
          border: "1px solid rgba(255,255,255,0.06)",
          backdropFilter: "blur(16px)",
        }}
      >
        <Link href="/" className="text-sm font-medium text-white/80 tracking-wide hover:text-white transition-colors">
          City Cycles
        </Link>
        <div className="flex items-center gap-4">
          <Link
            href="/compare"
            className="text-xs tracking-wide transition-colors"
            style={{
              color: pathname === "/compare" ? "rgba(255,255,255,0.9)" : "rgba(255,255,255,0.5)",
            }}
          >
            Compare
          </Link>
        </div>
      </div>
    </nav>
  );
}
