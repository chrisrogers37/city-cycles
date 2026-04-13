"use client";

import Link from "next/link";
import WeatherScene from "@/components/weather/WeatherScene";
import NavBar from "@/components/layout/NavBar";
import PageShell from "@/components/layout/PageShell";
import SimilarDayCard from "@/components/insights/SimilarDayCard";
import HourlyPatternChart from "@/components/insights/HourlyPatternChart";
import DurationInsight from "@/components/insights/DurationInsight";
import MemberCasualSplit from "@/components/insights/MemberCasualSplit";
import InsightCards from "@/components/insights/InsightCards";
import ForecastStrip from "@/components/insights/ForecastStrip";

const DISCOVER_LINKS = [
  {
    href: "/analytics",
    title: "Ride Analytics",
    desc: "238M+ rides analyzed",
    icon: (
      <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round">
        <path d="M3 3v18h18" /><path d="M7 16l4-8 4 4 5-10" />
      </svg>
    ),
  },
  {
    href: "/weather",
    title: "Weather Impact",
    desc: "How weather shapes cycling",
    icon: (
      <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round">
        <path d="M12 2v2m0 16v2M4.93 4.93l1.41 1.41m11.32 11.32l1.41 1.41M2 12h2m16 0h2M6.34 17.66l-1.41 1.41M19.07 4.93l-1.41 1.41" />
        <circle cx="12" cy="12" r="4" />
      </svg>
    ),
  },
  {
    href: "/stations",
    title: "Station Explorer",
    desc: "Performance by condition",
    icon: (
      <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round">
        <path d="M12 2C8.13 2 5 5.13 5 9c0 5.25 7 13 7 13s7-7.75 7-13c0-3.87-3.13-7-7-7z" />
        <circle cx="12" cy="9" r="2.5" />
      </svg>
    ),
  },
  {
    href: "/compare",
    title: "NYC vs London",
    desc: "Cross-city patterns",
    icon: (
      <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round">
        <path d="M16 3h5v5M8 3H3v5M16 21h5v-5M8 21H3v-5M12 3v18" />
      </svg>
    ),
  },
];

export default function LandingPage() {
  return (
    <>
      <NavBar />
      <WeatherScene />

      <PageShell>
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
          {DISCOVER_LINKS.map((link) => (
            <Link
              key={link.href}
              href={link.href}
              className="glass-card p-4 flex flex-col gap-2 hover:border-white/15 transition-all duration-300 group"
            >
              <span className="text-white/40 group-hover:text-[var(--color-neutral)] transition-colors">
                {link.icon}
              </span>
              <span className="text-sm text-white/90 font-medium">{link.title}</span>
              <span className="text-xs text-white/40">{link.desc}</span>
            </Link>
          ))}
        </div>

        <SimilarDayCard />
        <HourlyPatternChart />
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <DurationInsight />
          <MemberCasualSplit />
        </div>
        <InsightCards />
        <ForecastStrip />
      </PageShell>
    </>
  );
}
