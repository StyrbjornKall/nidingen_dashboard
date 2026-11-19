import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { ChartCard, type CardQuery } from "./ChartCard";

const q = <T,>(over: Partial<CardQuery<T>>): CardQuery<T> => ({
  data: undefined, isPending: false, isError: false, refetch: vi.fn(), ...over,
});

describe("ChartCard", () => {
  it("shows a skeleton and spinner while loading", () => {
    render(<ChartCard title="Ringmärkta över tid" query={q({ isPending: true })}>{() => "chart"}</ChartCard>);
    expect(screen.getByRole("status")).toHaveTextContent("Hämtar data …");
    expect(screen.getByRole("article")).toHaveAttribute("aria-busy", "true");
    expect(screen.queryByText("chart")).not.toBeInTheDocument();
  });

  it("shows an error with a retry that only affects this card", async () => {
    const refetch = vi.fn();
    render(<ChartCard title="Återfynd" query={q({ isError: true, refetch })}>{() => "chart"}</ChartCard>);
    expect(screen.getByRole("alert")).toHaveTextContent("Övriga diagram påverkas inte.");
    await userEvent.setup().click(screen.getByRole("button", { name: "Försök igen" }));
    expect(refetch).toHaveBeenCalledOnce();
  });

  it("explains an empty result and offers an action", async () => {
    const onClick = vi.fn();
    render(
      <ChartCard
        title="Medelvikt"
        query={q({ data: [] as number[] })}
        isEmpty={(d) => d.length === 0}
        emptyMessage="Inga vägda fåglar av den här arten under vald period."
        emptyAction={{ label: "Visa alla år", onClick }}
      >
        {() => "chart"}
      </ChartCard>,
    );
    expect(screen.getByText(/Inga vägda fåglar/)).toBeInTheDocument();
    await userEvent.setup().click(screen.getByRole("button", { name: "Visa alla år" }));
    expect(onClick).toHaveBeenCalledOnce();
  });

  it("renders the chart and the cached-data label", () => {
    render(
      <ChartCard title="Sammanfattning" query={q({ data: [1] })} cachedAt="2026-11-16T05:00:00">
        {(d) => `points: ${d.length}`}
      </ChartCard>,
    );
    expect(screen.getByText("points: 1")).toBeInTheDocument();
    expect(screen.getByText("Data från 16 nov 2026, 06:00")).toBeInTheDocument();
  });
});
