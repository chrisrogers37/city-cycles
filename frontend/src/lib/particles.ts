/**
 * Canvas particle system for weather effects.
 * Rain, snow, and lightning rendered via requestAnimationFrame.
 */

import type { WeatherCategory } from "./types";

// --- Particle configs by weather category ---

interface ParticleConfig {
  count: [number, number]; // [min, max] particles
  speed: [number, number]; // px/s range
  length: [number, number]; // px range (rain streak length / snow radius)
  opacity: [number, number];
  color: string;
  wind: number; // horizontal drift factor (0 = vertical)
}

const RAIN_CONFIG: Record<string, ParticleConfig> = {
  drizzle: {
    count: [30, 50],
    speed: [400, 800],
    length: [5, 12],
    opacity: [0.3, 0.5],
    color: "174,194,224",
    wind: 0.15,
  },
  rain: {
    count: [60, 100],
    speed: [600, 1200],
    length: [8, 20],
    opacity: [0.4, 0.6],
    color: "174,194,224",
    wind: 0.2,
  },
  heavy: {
    count: [120, 150],
    speed: [800, 1500],
    length: [15, 25],
    opacity: [0.5, 0.7],
    color: "174,194,224",
    wind: 0.25,
  },
  thunderstorm: {
    count: [120, 150],
    speed: [800, 1500],
    length: [15, 25],
    opacity: [0.5, 0.7],
    color: "174,194,224",
    wind: 0.25,
  },
};

const SNOW_CONFIG: ParticleConfig = {
  count: [50, 80],
  speed: [30, 80],
  length: [2, 5], // radius for snow circles
  opacity: [0.6, 0.9],
  color: "255,255,255",
  wind: 0.4,
};

// --- Utility ---

function rand(min: number, max: number): number {
  return min + Math.random() * (max - min);
}

function randInt(min: number, max: number): number {
  return Math.floor(rand(min, max + 1));
}

// --- Rain particle ---

interface RainDrop {
  x: number;
  y: number;
  speed: number;
  length: number;
  opacity: number;
}

function createRainDrop(w: number, h: number, cfg: ParticleConfig): RainDrop {
  return {
    x: rand(0, w),
    y: rand(-h, 0),
    speed: rand(cfg.speed[0], cfg.speed[1]),
    length: rand(cfg.length[0], cfg.length[1]),
    opacity: rand(cfg.opacity[0], cfg.opacity[1]),
  };
}

function updateRainDrop(
  drop: RainDrop,
  dt: number,
  w: number,
  h: number,
  cfg: ParticleConfig,
): void {
  drop.y += drop.speed * dt;
  drop.x += drop.speed * cfg.wind * dt;

  if (drop.y > h) {
    drop.y = rand(-20, 0);
    drop.x = rand(0, w);
    drop.speed = rand(cfg.speed[0], cfg.speed[1]);
    drop.length = rand(cfg.length[0], cfg.length[1]);
    drop.opacity = rand(cfg.opacity[0], cfg.opacity[1]);
  }
  if (drop.x > w) drop.x = 0;
}

function drawRainDrop(
  ctx: CanvasRenderingContext2D,
  drop: RainDrop,
  cfg: ParticleConfig,
): void {
  ctx.beginPath();
  ctx.moveTo(drop.x, drop.y);
  ctx.lineTo(drop.x + cfg.wind * drop.length, drop.y + drop.length);
  ctx.strokeStyle = `rgba(${cfg.color},${drop.opacity})`;
  ctx.lineWidth = 1.5;
  ctx.stroke();
}

// --- Snow particle ---

interface Snowflake {
  x: number;
  y: number;
  speed: number;
  radius: number;
  opacity: number;
  drift: number; // sinusoidal phase offset
  driftSpeed: number;
}

function createSnowflake(w: number, h: number): Snowflake {
  return {
    x: rand(0, w),
    y: rand(-h, 0),
    speed: rand(SNOW_CONFIG.speed[0], SNOW_CONFIG.speed[1]),
    radius: rand(SNOW_CONFIG.length[0], SNOW_CONFIG.length[1]),
    opacity: rand(SNOW_CONFIG.opacity[0], SNOW_CONFIG.opacity[1]),
    drift: rand(0, Math.PI * 2),
    driftSpeed: rand(0.5, 1.5),
  };
}

function updateSnowflake(
  flake: Snowflake,
  dt: number,
  t: number,
  w: number,
  h: number,
): void {
  flake.y += flake.speed * dt;
  flake.x += Math.sin(t * flake.driftSpeed + flake.drift) * SNOW_CONFIG.wind * 30 * dt;

  if (flake.y > h + 10) {
    flake.y = rand(-20, 0);
    flake.x = rand(0, w);
    flake.speed = rand(SNOW_CONFIG.speed[0], SNOW_CONFIG.speed[1]);
    flake.radius = rand(SNOW_CONFIG.length[0], SNOW_CONFIG.length[1]);
    flake.opacity = rand(SNOW_CONFIG.opacity[0], SNOW_CONFIG.opacity[1]);
  }
  if (flake.x > w + 10) flake.x = -10;
  if (flake.x < -10) flake.x = w + 10;
}

function drawSnowflake(
  ctx: CanvasRenderingContext2D,
  flake: Snowflake,
): void {
  ctx.beginPath();
  ctx.arc(flake.x, flake.y, flake.radius, 0, Math.PI * 2);
  ctx.fillStyle = `rgba(${SNOW_CONFIG.color},${flake.opacity})`;
  ctx.fill();
}

// --- Lightning ---

interface LightningBolt {
  segments: Array<{ x1: number; y1: number; x2: number; y2: number }>;
  startTime: number;
  holdMs: number;
  afterglowMs: number;
}

function createLightningBolt(w: number, h: number, now: number): LightningBolt {
  const segments: LightningBolt["segments"] = [];
  const startX = rand(w * 0.15, w * 0.85);
  let x = startX;
  let y = 0;
  const targetY = h * rand(0.5, 0.8);
  const stepCount = randInt(6, 12);
  const stepY = targetY / stepCount;

  for (let i = 0; i < stepCount; i++) {
    const nextX = x + rand(-40, 40);
    const nextY = y + stepY + rand(-10, 10);
    segments.push({ x1: x, y1: y, x2: nextX, y2: nextY });

    // Branch with 25% chance
    if (Math.random() < 0.25) {
      const branchLen = rand(20, 60);
      const branchAngle = rand(-Math.PI / 3, Math.PI / 3);
      segments.push({
        x1: nextX,
        y1: nextY,
        x2: nextX + Math.sin(branchAngle) * branchLen,
        y2: nextY + Math.cos(branchAngle) * branchLen,
      });
    }

    x = nextX;
    y = nextY;
  }

  return { segments, startTime: now, holdMs: 100, afterglowMs: 200 };
}

function drawLightning(
  ctx: CanvasRenderingContext2D,
  bolt: LightningBolt,
  now: number,
): boolean {
  const elapsed = now - bolt.startTime;
  const totalMs = bolt.holdMs + bolt.afterglowMs;
  if (elapsed > totalMs) return false; // done

  let alpha: number;
  if (elapsed < bolt.holdMs) {
    alpha = 1;
  } else {
    alpha = 1 - (elapsed - bolt.holdMs) / bolt.afterglowMs;
  }

  // Glow layer
  ctx.save();
  ctx.shadowColor = `rgba(200, 220, 255, ${alpha * 0.8})`;
  ctx.shadowBlur = 20;
  ctx.strokeStyle = `rgba(255, 255, 255, ${alpha})`;
  ctx.lineWidth = 3;
  ctx.beginPath();
  for (const seg of bolt.segments) {
    ctx.moveTo(seg.x1, seg.y1);
    ctx.lineTo(seg.x2, seg.y2);
  }
  ctx.stroke();
  ctx.restore();

  // Core bright line
  ctx.strokeStyle = `rgba(255, 255, 255, ${alpha})`;
  ctx.lineWidth = 1.5;
  ctx.beginPath();
  for (const seg of bolt.segments) {
    ctx.moveTo(seg.x1, seg.y1);
    ctx.lineTo(seg.x2, seg.y2);
  }
  ctx.stroke();

  return true; // still active
}

// --- Screen flash for lightning ---

function drawFlash(
  ctx: CanvasRenderingContext2D,
  bolt: LightningBolt,
  now: number,
  w: number,
  h: number,
): void {
  const elapsed = now - bolt.startTime;
  if (elapsed > bolt.holdMs) return;
  const alpha = 0.15 * (1 - elapsed / bolt.holdMs);
  ctx.fillStyle = `rgba(200, 220, 255, ${alpha})`;
  ctx.fillRect(0, 0, w, h);
}

// --- Main particle system ---

export interface ParticleSystemState {
  rainDrops: RainDrop[];
  snowflakes: Snowflake[];
  lightning: LightningBolt[];
  lastLightningTime: number;
  shakeOffset: { x: number; y: number };
}

export function createParticleSystem(
  category: WeatherCategory,
  w: number,
  h: number,
  isMobile: boolean,
): ParticleSystemState {
  const mobileFactor = isMobile ? 0.5 : 1;
  const state: ParticleSystemState = {
    rainDrops: [],
    snowflakes: [],
    lightning: [],
    lastLightningTime: 0,
    shakeOffset: { x: 0, y: 0 },
  };

  if (category === "rain" || category === "drizzle" || category === "thunderstorm") {
    const cfg = getRainConfig(category);
    const count = Math.round(randInt(cfg.count[0], cfg.count[1]) * mobileFactor);
    for (let i = 0; i < count; i++) {
      state.rainDrops.push(createRainDrop(w, h, cfg));
    }
  }

  if (category === "snow") {
    const count = Math.round(
      randInt(SNOW_CONFIG.count[0], SNOW_CONFIG.count[1]) * mobileFactor,
    );
    for (let i = 0; i < count; i++) {
      state.snowflakes.push(createSnowflake(w, h));
    }
  }

  return state;
}

function getRainConfig(category: WeatherCategory): ParticleConfig {
  if (category === "drizzle") return RAIN_CONFIG.drizzle;
  if (category === "thunderstorm") return RAIN_CONFIG.thunderstorm;
  return RAIN_CONFIG.rain;
}

/** Returns true if the category uses canvas particles (false = CSS-only). */
export function needsCanvas(category: WeatherCategory | undefined): boolean {
  return (
    category === "rain" ||
    category === "drizzle" ||
    category === "snow" ||
    category === "thunderstorm"
  );
}

export function updateAndDraw(
  ctx: CanvasRenderingContext2D,
  state: ParticleSystemState,
  category: WeatherCategory,
  dt: number,
  t: number,
  w: number,
  h: number,
): void {
  ctx.clearRect(0, 0, w, h);

  // Rain / drizzle / thunderstorm rain
  if (state.rainDrops.length > 0) {
    const cfg = getRainConfig(category);
    for (const drop of state.rainDrops) {
      updateRainDrop(drop, dt, w, h, cfg);
      drawRainDrop(ctx, drop, cfg);
    }
  }

  // Snow
  for (const flake of state.snowflakes) {
    updateSnowflake(flake, dt, t, w, h);
    drawSnowflake(ctx, flake);
  }

  // Thunderstorm lightning
  if (category === "thunderstorm") {
    const now = t * 1000; // convert to ms-like for timing

    // Spawn lightning every 3-8 seconds
    if (now - state.lastLightningTime > rand(3000, 8000)) {
      state.lightning.push(createLightningBolt(w, h, now));
      state.lastLightningTime = now;
    }

    // Draw active lightning bolts
    state.lightning = state.lightning.filter((bolt) => {
      drawFlash(ctx, bolt, now, w, h);
      return drawLightning(ctx, bolt, now);
    });

    // Screen shake
    if (state.lightning.length > 0) {
      const newest = state.lightning[state.lightning.length - 1];
      const elapsed = now - newest.startTime;
      if (elapsed < 150) {
        state.shakeOffset.x = rand(-2, 2);
        state.shakeOffset.y = rand(-2, 2);
      } else {
        state.shakeOffset.x = 0;
        state.shakeOffset.y = 0;
      }
    } else {
      state.shakeOffset.x = 0;
      state.shakeOffset.y = 0;
    }
  }
}
