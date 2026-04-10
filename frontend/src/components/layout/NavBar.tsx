"use client";

import { useScrollPosition } from "@/hooks/useScrollPosition";

/** Floating nav bar — hidden on landing until scrolled past the weather scene. */
export default function NavBar() {
  const scrollY = useScrollPosition();
  const visible = scrollY > (typeof window !== "undefined" ? window.innerHeight * 0.8 : 600);

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
        <span className="text-sm font-medium text-white/80 tracking-wide">City Cycles</span>
        {/* Nav items added in Phase 04/05 when pages exist */}
      </div>
    </nav>
  );
}
