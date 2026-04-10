interface Props {
  children: React.ReactNode;
}

/** Dark-themed wrapper for content below the weather scene. */
export default function PageShell({ children }: Props) {
  return (
    <div
      className="relative z-20"
      style={{ background: "#0d1117" }}
    >
      <div className="mx-auto max-w-5xl px-4 sm:px-6 py-12 flex flex-col gap-8">
        {children}
      </div>
    </div>
  );
}
