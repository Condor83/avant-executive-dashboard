import { describe, expect, it } from "vitest";
import {
  formatUSD,
  formatUSDCompact,
  formatPercent,
  formatROE,
  formatAPY,
  formatDate,
  formatAge,
  formatRatio,
  financialColor,
} from "../lib/formatters";

describe("formatUSD", () => {
  it("formats a normal value", () => {
    expect(formatUSD("12345.67")).toBe("$12,345.67");
  });
  it("formats zero", () => {
    expect(formatUSD("0")).toBe("$0.00");
  });
  it("formats negative value", () => {
    expect(formatUSD("-500.5")).toBe("-$500.50");
  });
  it("returns fallback for null", () => {
    expect(formatUSD(null)).toBe("---");
  });
  it("returns fallback for undefined", () => {
    expect(formatUSD(undefined)).toBe("---");
  });
  it("returns fallback for empty string", () => {
    expect(formatUSD("")).toBe("---");
  });
  it("returns fallback for non-numeric string", () => {
    expect(formatUSD("abc")).toBe("---");
  });
});

describe("formatUSDCompact", () => {
  it("formats millions", () => {
    expect(formatUSDCompact("5432100")).toBe("$5.43M");
  });
  it("formats thousands", () => {
    expect(formatUSDCompact("75000")).toBe("$75.0K");
  });
  it("formats small values as full USD", () => {
    expect(formatUSDCompact("999")).toBe("$999.00");
  });
  it("handles negative millions", () => {
    expect(formatUSDCompact("-2000000")).toBe("-$2.00M");
  });
  it("returns fallback for null", () => {
    expect(formatUSDCompact(null)).toBe("---");
  });
});

describe("formatPercent", () => {
  it("formats a decimal as percentage", () => {
    expect(formatPercent("0.1534")).toBe("15.34%");
  });
  it("formats zero", () => {
    expect(formatPercent("0")).toBe("0.00%");
  });
  it("formats 100%", () => {
    expect(formatPercent("1")).toBe("100.00%");
  });
  it("returns fallback for null", () => {
    expect(formatPercent(null)).toBe("---");
  });
});

describe("formatROE", () => {
  it("formats a positive ROE", () => {
    expect(formatROE("0.0523")).toBe("5.23%");
  });
  it("formats a negative ROE", () => {
    expect(formatROE("-0.01")).toBe("-1.00%");
  });
  it("returns fallback for null", () => {
    expect(formatROE(null)).toBe("---");
  });
});

describe("formatAPY", () => {
  it("formats a normal APY", () => {
    expect(formatAPY("0.045")).toBe("4.50%");
  });
  it("returns fallback for null", () => {
    expect(formatAPY(null)).toBe("---");
  });
});

describe("formatDate", () => {
  it("formats an ISO date", () => {
    const result = formatDate("2025-06-15T12:00:00Z");
    expect(result).toContain("Jun");
    expect(result).toContain("15");
    expect(result).toContain("2025");
  });
  it("returns fallback for null", () => {
    expect(formatDate(null)).toBe("---");
  });
  it("returns fallback for empty", () => {
    expect(formatDate("")).toBe("---");
  });
});

describe("formatAge", () => {
  it("formats minutes for < 1 hour", () => {
    expect(formatAge(0.5)).toBe("30m ago");
  });
  it("formats hours for < 24 hours", () => {
    expect(formatAge(3.5)).toBe("3.5h ago");
  });
  it("formats days for >= 24 hours", () => {
    expect(formatAge(48)).toBe("2.0d ago");
  });
  it("returns fallback for null", () => {
    expect(formatAge(null)).toBe("---");
  });
});

describe("formatRatio", () => {
  it("formats a ratio", () => {
    expect(formatRatio("1.53")).toBe("1.53x");
  });
  it("returns fallback for null", () => {
    expect(formatRatio(null)).toBe("---");
  });
});

describe("financialColor", () => {
  it("returns green for positive", () => {
    expect(financialColor("100")).toBe("text-avant-success");
  });
  it("returns red for negative", () => {
    expect(financialColor("-50")).toBe("text-avant-danger");
  });
  it("returns neutral for zero", () => {
    expect(financialColor("0")).toBe("text-foreground");
  });
  it("returns neutral for null", () => {
    expect(financialColor(null)).toBe("text-foreground");
  });
});
