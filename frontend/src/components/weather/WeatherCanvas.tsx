"use client";

import { useRef, useEffect, useCallback } from "react";
import type { WeatherCategory } from "@/lib/types";
import {
  createParticleSystem,
  updateAndDraw,
  type ParticleSystemState,
} from "@/lib/particles";

interface WeatherCanvasProps {
  category: WeatherCategory;
}

export default function WeatherCanvas({ category }: WeatherCanvasProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const stateRef = useRef<ParticleSystemState | null>(null);
  const animRef = useRef<number>(0);
  const prevTimeRef = useRef<number>(0);
  const categoryRef = useRef(category);
  const containerRef = useRef<HTMLDivElement>(null);

  // Re-init particles when category changes
  const initParticles = useCallback(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const w = canvas.width;
    const h = canvas.height;
    const isMobile = window.innerWidth < 768;
    stateRef.current = createParticleSystem(category, w, h, isMobile);
    categoryRef.current = category;
  }, [category]);

  useEffect(() => {
    const canvas = canvasRef.current;
    const container = containerRef.current;
    if (!canvas || !container) return;

    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    // Size canvas to container
    const resize = () => {
      const rect = container.getBoundingClientRect();
      const dpr = window.devicePixelRatio || 1;
      canvas.width = rect.width * dpr;
      canvas.height = rect.height * dpr;
      canvas.style.width = `${rect.width}px`;
      canvas.style.height = `${rect.height}px`;
      ctx.scale(dpr, dpr);
      initParticles();
    };

    resize();

    const resizeObserver = new ResizeObserver(resize);
    resizeObserver.observe(container);

    // Animation loop
    const tick = (timestamp: number) => {
      if (!stateRef.current) {
        animRef.current = requestAnimationFrame(tick);
        return;
      }

      if (prevTimeRef.current === 0) prevTimeRef.current = timestamp;
      const dt = Math.min((timestamp - prevTimeRef.current) / 1000, 0.1); // cap at 100ms
      prevTimeRef.current = timestamp;

      const t = timestamp / 1000; // seconds since start
      const rect = container.getBoundingClientRect();

      // Reset transform for HiDPI
      ctx.setTransform(1, 0, 0, 1, 0, 0);
      const dpr = window.devicePixelRatio || 1;
      ctx.scale(dpr, dpr);

      updateAndDraw(ctx, stateRef.current, categoryRef.current, dt, t, rect.width, rect.height);

      // Apply screen shake for thunderstorm
      if (stateRef.current.shakeOffset.x !== 0 || stateRef.current.shakeOffset.y !== 0) {
        container.style.transform = `translate(${stateRef.current.shakeOffset.x}px, ${stateRef.current.shakeOffset.y}px)`;
      } else {
        container.style.transform = "";
      }

      animRef.current = requestAnimationFrame(tick);
    };

    animRef.current = requestAnimationFrame(tick);

    return () => {
      cancelAnimationFrame(animRef.current);
      resizeObserver.disconnect();
      if (container) container.style.transform = "";
    };
  }, [initParticles]);

  // Re-init when category changes
  useEffect(() => {
    if (categoryRef.current !== category) {
      initParticles();
    }
  }, [category, initParticles]);

  return (
    <div ref={containerRef} className="absolute inset-0 pointer-events-none overflow-hidden">
      <canvas ref={canvasRef} className="block w-full h-full" />
    </div>
  );
}
