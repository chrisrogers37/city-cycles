"use client";

import { useState, useMemo } from "react";

interface Column<T> {
  key: keyof T & string;
  label: string;
  format?: (value: T[keyof T], row: T) => string;
  align?: "left" | "right";
}

interface DataTableProps<T> {
  columns: Column<T>[];
  data: T[];
  defaultSortKey?: keyof T & string;
  defaultSortDir?: "asc" | "desc";
}

export default function DataTable<T extends Record<string, unknown>>({
  columns,
  data,
  defaultSortKey,
  defaultSortDir = "desc",
}: DataTableProps<T>) {
  const [sortKey, setSortKey] = useState<string>(defaultSortKey ?? columns[0]?.key ?? "");
  const [sortDir, setSortDir] = useState<"asc" | "desc">(defaultSortDir);

  const sorted = useMemo(() => {
    if (!sortKey) return data;
    return [...data].sort((a, b) => {
      const av = a[sortKey];
      const bv = b[sortKey];
      if (av == null && bv == null) return 0;
      if (av == null) return 1;
      if (bv == null) return -1;
      const cmp = typeof av === "number" && typeof bv === "number"
        ? av - bv
        : String(av).localeCompare(String(bv));
      return sortDir === "asc" ? cmp : -cmp;
    });
  }, [data, sortKey, sortDir]);

  function handleSort(key: string) {
    if (key === sortKey) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(key);
      setSortDir("desc");
    }
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-white/8">
            {columns.map((col) => (
              <th
                key={col.key}
                onClick={() => handleSort(col.key)}
                className="py-2 px-3 font-medium text-white/50 cursor-pointer hover:text-white/70 transition-colors select-none"
                style={{ textAlign: col.align ?? "left" }}
              >
                {col.label}
                {sortKey === col.key && (
                  <span className="ml-1">{sortDir === "asc" ? "\u25B2" : "\u25BC"}</span>
                )}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {sorted.map((row, i) => (
            <tr key={i} className="border-b border-white/4 hover:bg-white/[0.02] transition-colors">
              {columns.map((col) => (
                <td
                  key={col.key}
                  className="py-2 px-3 text-white/70"
                  style={{ textAlign: col.align ?? "left" }}
                >
                  {col.format ? col.format(row[col.key], row) : String(row[col.key] ?? "—")}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
