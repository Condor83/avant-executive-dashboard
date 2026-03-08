import { fireEvent, render, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { DataTable, type Column } from "../components/shared/data-table";

type Row = {
  label: string;
  value: number;
  secondary: number;
};

const columns: Column<Row>[] = [
  {
    key: "label",
    header: "Label",
    cell: (row) => row.label,
  },
  {
    key: "value",
    header: "Value",
    sortable: true,
    sortValue: (row) => row.value,
    cell: (row) => String(row.value),
  },
  {
    key: "secondary",
    header: "Secondary",
    sortable: true,
    sortValue: (row) => row.secondary,
    cell: (row) => String(row.secondary),
  },
];

const rows: Row[] = [
  { label: "alpha", value: 1, secondary: 30 },
  { label: "beta", value: 3, secondary: 10 },
  { label: "gamma", value: 2, secondary: 20 },
];

function renderedOrder() {
  const tbody = document.querySelector("tbody");
  if (!tbody) {
    throw new Error("tbody not found");
  }
  return within(tbody)
    .getAllByRole("row")
    .map((row) => row.textContent ?? "");
}

function headerCell(label: string) {
  const thead = document.querySelector("thead");
  if (!thead) {
    throw new Error("thead not found");
  }
  return within(thead).getByText(label);
}

describe("DataTable", () => {
  it("applies the initial sort descending", () => {
    render(
      <DataTable
        columns={columns}
        data={rows}
        rowKey={(row) => row.label}
        initialSortKey="value"
        initialSortDir="desc"
      />,
    );

    expect(renderedOrder()).toEqual(["beta310", "gamma220", "alpha130"]);
  });

  it("toggles the same sortable header between asc and desc", () => {
    render(
      <DataTable
        columns={columns}
        data={rows}
        rowKey={(row) => row.label}
        initialSortKey="value"
        initialSortDir="desc"
      />,
    );

    fireEvent.click(headerCell("Value"));
    expect(renderedOrder()).toEqual(["alpha130", "gamma220", "beta310"]);

    fireEvent.click(headerCell("Value"));
    expect(renderedOrder()).toEqual(["beta310", "gamma220", "alpha130"]);
  });

  it("resets a new sortable column to descending and ignores non-sortable headers", () => {
    render(
      <DataTable
        columns={columns}
        data={rows}
        rowKey={(row) => row.label}
        initialSortKey="value"
        initialSortDir="desc"
      />,
    );

    fireEvent.click(headerCell("Secondary"));
    expect(renderedOrder()).toEqual(["alpha130", "gamma220", "beta310"]);

    fireEvent.click(headerCell("Label"));
    expect(renderedOrder()).toEqual(["alpha130", "gamma220", "beta310"]);
  });
});
