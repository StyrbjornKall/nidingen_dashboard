import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { describe, expect, it } from "vitest";
import type { SpeciesInfo } from "../lib/api";
import { speciesPalette } from "../lib/tokens";
import { matchesQuery, SpeciesPicker } from "./SpeciesPicker";

const sp = (code: string, name: string, name_sci: string | null): SpeciesInfo => ({
  code, name, name_sci, name_sv: name.toLowerCase(), name_en: null, order: null, family: null,
  n_new_total: 1, n_recap_total: 0, share_var: 0.5, single_season: false, single_season_season: null,
});

const SPECIES = [
  sp("ALLA", "Alla arter", null),
  sp("RÖHAK", "Rödhake", "Erithacus rubecula"),
  sp("LÖSÅN", "Lövsångare", "Phylloscopus trochilus"),
  sp("KUFÅG", "Kungsfågel", "Regulus regulus"),
];

function Harness({ initial = [] as string[], max = 6, allowAll = true }) {
  const [selected, setSelected] = useState(initial);
  const toggle = (c: string) => setSelected((s) => (s.includes(c) ? s.filter((x) => x !== c) : [...s, c]));
  return (
    <>
      <SpeciesPicker species={SPECIES} selected={selected} onToggle={toggle} max={max} allowAll={allowAll} />
      <output data-testid="selected">{selected.join(",")}</output>
    </>
  );
}

describe("matchesQuery", () => {
  it("searches code, Swedish and scientific name", () => {
    expect(matchesQuery(SPECIES[1]!, "röh")).toBe(true);
    expect(matchesQuery(SPECIES[1]!, "erith")).toBe(true);
    expect(matchesQuery(SPECIES[1]!, "LÖSÅN")).toBe(false);
  });
});

describe("SpeciesPicker", () => {
  it("adds species in order with colours by selection order", async () => {
    const user = userEvent.setup();
    render(<Harness />);
    await user.click(screen.getByRole("button", { name: "Välj art" }));
    expect(screen.getByLabelText("Sök art")).toHaveFocus();
    await user.click(screen.getByRole("button", { name: /Lövsångare/ }));
    await user.click(screen.getByRole("button", { name: /Rödhake/ }));
    expect(screen.getByTestId("selected")).toHaveTextContent("LÖSÅN,RÖHAK");
    const chip = screen.getByRole("button", { name: "Ta bort Rödhake" }).parentElement!;
    expect(chip).toHaveStyle({ borderColor: speciesPalette[1] });
  });

  it("filters by search and says when nothing matches", async () => {
    const user = userEvent.setup();
    render(<Harness />);
    await user.click(screen.getByRole("button", { name: "Välj art" }));
    await user.type(screen.getByLabelText("Sök art"), "regulus");
    expect(screen.getByRole("button", { name: /Kungsfågel/ })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Rödhake/ })).not.toBeInTheDocument();
    await user.clear(screen.getByLabelText("Sök art"));
    await user.type(screen.getByLabelText("Sök art"), "zzz");
    expect(screen.getByText("Ingen art matchar.")).toBeInTheDocument();
  });

  it("disables other species when the maximum is reached", async () => {
    const user = userEvent.setup();
    render(<Harness initial={["RÖHAK", "LÖSÅN"]} max={2} />);
    await user.click(screen.getByRole("button", { name: "Lägg till art" }));
    expect(screen.getByRole("button", { name: /Kungsfågel/ })).toBeDisabled();
    expect(screen.getByText(/2 av 2 valda — ta bort en för att byta/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Ta bort Rödhake" }));
    expect(screen.getByRole("button", { name: /Kungsfågel/ })).toBeEnabled();
  });

  it("closes on Escape and returns focus to the button", async () => {
    const user = userEvent.setup();
    render(<Harness />);
    const button = screen.getByRole("button", { name: "Välj art" });
    await user.click(button);
    await user.keyboard("{Escape}");
    expect(screen.queryByLabelText("Sök art")).not.toBeInTheDocument();
    expect(button).toHaveFocus();
  });

  it("can leave out Alla arter", async () => {
    const user = userEvent.setup();
    render(<Harness allowAll={false} />);
    await user.click(screen.getByRole("button", { name: "Välj art" }));
    expect(screen.queryByRole("button", { name: /Alla arter/ })).not.toBeInTheDocument();
  });
});
