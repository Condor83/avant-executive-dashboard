"use client";

import { useState, useMemo, type ReactNode } from "react";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Skeleton } from "@/components/ui/skeleton";
import { ArrowUpDown, ArrowUp, ArrowDown } from "lucide-react";
import { cn } from "@/lib/utils";

export interface Column<T> {
  key: string;
  header: string;
  cell: (row: T) => ReactNode;
  sortable?: boolean;
  align?: "left" | "right";
  sortFn?: (a: T, b: T) => number;
  sortValue?: (row: T) => number | string | null | undefined;
  headerClassName?: string;
  cellClassName?: string;
}

interface DataTableProps<T> {
  columns: Column<T>[];
  data: T[];
  isLoading?: boolean;
  emptyMessage?: string;
  onRowClick?: (row: T) => void;
  filterSlot?: ReactNode;
  rowKey: (row: T) => string;
  initialSortKey?: string;
  initialSortDir?: "asc" | "desc";
}

function compareSortValues(
  left: number | string,
  right: number | string,
): number {
  if (typeof left === "number" && typeof right === "number") {
    return left - right;
  }
  return String(left).localeCompare(String(right));
}

export function DataTable<T>({
  columns,
  data,
  isLoading,
  emptyMessage = "No data available",
  onRowClick,
  filterSlot,
  rowKey,
  initialSortKey,
  initialSortDir = "desc",
}: DataTableProps<T>) {
  const [sortKey, setSortKey] = useState<string | null>(initialSortKey ?? null);
  const [sortDir, setSortDir] = useState<"asc" | "desc">(initialSortDir);

  const sorted = useMemo(() => {
    if (!sortKey) return data;
    const col = columns.find((c) => c.key === sortKey);
    if (col?.sortValue) {
      const sortableRows: T[] = [];
      const nullRows: T[] = [];

      for (const row of data) {
        const value = col.sortValue(row);
        if (value === null || value === undefined || value === "") {
          nullRows.push(row);
        } else {
          sortableRows.push(row);
        }
      }

      const sortedRows = [...sortableRows].sort((a, b) => {
        const left = col.sortValue!(a);
        const right = col.sortValue!(b);
        return compareSortValues(left as number | string, right as number | string);
      });
      if (sortDir === "desc") {
        sortedRows.reverse();
      }
      return [...sortedRows, ...nullRows];
    }
    if (!col?.sortFn) return data;
    const mul = sortDir === "asc" ? 1 : -1;
    return [...data].sort((a, b) => mul * col.sortFn!(a, b));
  }, [data, sortKey, sortDir, columns]);

  function handleSort(key: string) {
    if (sortKey === key) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(key);
      setSortDir("desc");
    }
  }

  if (isLoading) {
    return (
      <div className="space-y-2">
        {filterSlot}
        {Array.from({ length: 5 }).map((_, i) => (
          <Skeleton key={i} className="h-10 w-full" />
        ))}
      </div>
    );
  }

  return (
    <div>
      {filterSlot}
      <div className="bg-card">
        <Table>
          <TableHeader>
            <TableRow>
              {columns.map((col) => (
                <TableHead
                  key={col.key}
                  className={cn(
                    col.align === "right" && "text-right",
                    col.sortable && "cursor-pointer select-none",
                    col.headerClassName,
                  )}
                  onClick={col.sortable ? () => handleSort(col.key) : undefined}
                >
                  <span className="inline-flex items-center gap-1">
                    {col.header}
                    {col.sortable && sortKey === col.key ? (
                      sortDir === "asc" ? (
                        <ArrowUp className="h-3 w-3" />
                      ) : (
                        <ArrowDown className="h-3 w-3" />
                      )
                    ) : col.sortable ? (
                      <ArrowUpDown className="h-3 w-3 text-slate-400" />
                    ) : null}
                  </span>
                </TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {sorted.length === 0 ? (
              <TableRow>
                <TableCell
                  colSpan={columns.length}
                  className="py-8 text-center text-sm text-slate-500"
                >
                  {emptyMessage}
                </TableCell>
              </TableRow>
            ) : (
              sorted.map((row) => (
                <TableRow
                  key={rowKey(row)}
                  className={cn(onRowClick && "cursor-pointer hover:bg-slate-50")}
                  onClick={onRowClick ? () => onRowClick(row) : undefined}
                >
                  {columns.map((col) => (
                    <TableCell
                      key={col.key}
                      className={cn(
                        "text-sm tabular-nums",
                        col.align === "right" && "text-right",
                        col.cellClassName,
                      )}
                    >
                      {col.cell(row)}
                    </TableCell>
                  ))}
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}
