/**
 * The texts of Om. Everything that is a number, a year or a season boundary
 * is built from the API (`Facts`), so the page cannot go out of date when a
 * new season is added or the config changes. The week and weather texts are
 * the wording decided in design/README.md and config/analysis.json.
 */
import { formatDate, formatDateTime, formatRounded, monthName } from "../../lib/format";

export interface Facts {
  nNew: number;
  nRecap: number;
  nRecoveries: number;
  nSpecies: number;
  firstYear: number;
  currentYear: number;
  completeThrough: number;
  maxCompare: number;
  gap: { yearFrom: number; yearTo: number; reason: string };
  seasons: { id: string; months: number[] }[];
  weatherNote: string;
  ringingMaxDate: string;
  builtAtUtc: string;
}

export interface Term {
  term: string;
  text: string;
}

export interface Qa {
  id: string;
  q: string;
  a: string;
}

/** "januari–juni" for the months of one season. */
export function monthSpan(months: readonly number[]): string {
  const first = months[0]!;
  const last = months[months.length - 1]!;
  return `${monthName(first, true)}–${monthName(last, true)}`;
}

function seasonSentence(f: Facts): string {
  const span = (id: string) => {
    const s = f.seasons.find((x) => x.id === id);
    return s ? monthSpan(s.months) : "";
  };
  return `Vår är ${span("var")} och höst är ${span("host")}.`;
}

/** The second paragraph of the introduction. */
export function materialSentence(f: Facts): string {
  return (
    `Materialet omfattar omkring ${formatRounded(f.nNew)} nyringmärkta fåglar, ${formatRounded(f.nRecap)} kontroller på ön och ` +
    `${formatRounded(f.nRecoveries)} återfynd från hela världen, tillsammans med väderdata från SMHI. ` +
    "Siffrorna uppdateras när en ny säsong har förts in i stationens databas."
  );
}

export function glossary(f: Facts): Term[] {
  return [
    { term: "Märkning", text: "En fågel fångas, får en ring med unikt nummer runt benet, mäts och släpps igen." },
    { term: "Ringmärkta", text: "Antalet nya märkningar. Kontroller av redan märkta fåglar räknas aldrig in i det." },
    { term: "Kontroll", text: "En redan ringmärkt fågel fångas på nytt här på Nidingen." },
    {
      term: "Återfynd",
      text: "En fågel märkt på Nidingen som hittas någon annanstans (utgående), eller en fågel märkt någon annanstans som fångas här (inkommande).",
    },
    { term: "Säsong", text: `${seasonSentence(f)} Samma gränser gäller på alla sidor.` },
    { term: "Medianpassage", text: "Dagen då hälften av säsongens fåglar av en art har passerat. Medianen räknas för varje år och säsong, och sidorna visar medelvärdet över åren." },
    { term: "Vecka", text: "Sju dagar räknat från 1 januari (vecka 1 = 1–7 januari), inte kalendervecka." },
    { term: "Kön", text: "Hane, hona eller okänd. Många arter går inte att könsbestämma i handen, så okänd är vanligt. Sällsynta koder visas som de står i databasen." },
    {
      term: "Ålderskoder",
      text:
        "Åldern visas med koderna i databasen, utan gruppering: 10 är född i år (1K), 20 föddes i fjol (2K), och ett plustecken betyder minst så gammal, " +
        "till exempel 2+ för minst två kalenderår. 1+ är fullvuxen fågel med okänd ålder. Okänd betyder att åldern inte är noterad.",
    },
  ];
}

export const WEEKS_ANSWER =
  "När data visas per vecka använder vi inte kalenderveckor. En vecka här är en period på sju dagar räknad från den 1 januari: vecka 1 är 1–7 januari, " +
  "vecka 2 är 8–14 januari och så vidare. Flyttfåglarna bryr sig inte om veckonummer, och på det här sättet hamnar samma datum i samma vecka varje år, " +
  "så att åren går att jämföra. Vecka 12 på webbplatsen är alltså alltid 19–25 mars, inte kalendervecka 12. Vecka 52 omfattar årets sista åtta dagar " +
  "(24–31 december), och skottdagen 29 februari räknas till samma vecka som 28 februari.";

export function faq(f: Facts): Qa[] {
  const items: Qa[] = [
    {
      id: "jamfor",
      q: "Hur jämför jag flera arter?",
      a:
        `Använd artväljaren högst upp på sidorna Sammanfattning, Trender, Morfometri och Fenologi. Du kan välja upp till ${f.maxCompare} arter. ` +
        "Varje art får en egen färg som är densamma i alla diagram, och ditt val följer med när du byter sida. Du kan också klicka på en art i listorna " +
        "för att lägga till eller ta bort den.",
    },
    {
      id: "filter",
      q: "Var gäller filtren?",
      a:
        "Raden som börjar med ”Visar” eller ”Jämför” gäller alla diagram på sidan. Återfynd och Väder har egna " +
        "kontroller och påverkas inte av artväljaren, eftersom de bygger på andra data. Diagrammet över innevarande säsong på Sammanfattning " +
        "följer inte periodvalet.",
    },
    {
      id: "band",
      q: "Vad betyder det skuggade bandet runt trendlinjen?",
      a:
        "Det är ett 95 % konfidensintervall för linjen. Om hela bandet lutar åt samma håll är trenden tydlig. Om bandet rymmer både uppåt och " +
        "nedåt kan vi inte säga säkert att något har förändrats.",
    },
    {
      id: "delvis",
      q: `Varför saknas ${f.gap.yearFrom}–${f.gap.yearTo} i trenderna?`,
      a:
        `${f.gap.reason} Därför är årens fångst inte jämförbar med övriga år. De syns i diagram över enskilda år, märkta ”delvis bemannad”, men ingår inte i ` +
        "trendlinjer, medelvärden, standardavvikelser, toppår och sämsta år eller medianpassage. På Väder går de att välja, men fångsten är lägre än ett vanligt år.",
    },
  ];
  if (f.currentYear > f.completeThrough) {
    items.push({
      id: "innevarande",
      q: `Varför är ${f.currentYear} inte med i trenderna?`,
      a:
        `Trenderna räknas på hela säsonger. Data för ${f.currentYear} går till och med ${formatDate(f.ringingMaxDate, true)}, så året jämförs i stället med ` +
        `tidigare år på sidan Sammanfattning, under ”Säsongen ${f.currentYear}”.`,
    });
  }
  items.push(
    { id: "veckor", q: "Varför stämmer inte veckorna med almanackan?", a: WEEKS_ANSWER },
    {
      id: "timmar",
      q: "Vilka timmar gäller väderuppgifterna?",
      a: f.weatherNote,
    },
    {
      id: "vader",
      q: "Varifrån kommer väderdatan?",
      a:
        "Från SMHI:s station Nidingen A. Där stationen inte längre mäter, eller värden saknas, fylls de i från Vinga A. Det gäller framför allt nederbörd, " +
        "lufttryck och sikt, och värdena är markerade som Vinga A på sidan Väder.",
    },
    {
      id: "arter",
      q: "Vad menas med unika arter och raser?",
      a:
        `Databasen har ${f.nSpecies} olika artkoder. Förutom arter finns raser (underarter) och former som ringmärkaren har kunnat skilja åt, ` +
        "och de räknas var för sig. Några få koder saknar svenskt namn och visas då med koden.",
    },
    {
      id: "uppdatering",
      q: "Hur ofta uppdateras siffrorna?",
      a:
        `När en ny säsong har förts in i stationens databas. Ringmärkningsdatan går just nu till och med ${formatDate(f.ringingMaxDate, true)}, ` +
        `och sidan byggdes ${formatDateTime(f.builtAtUtc)}.`,
    },
    {
      id: "personer",
      q: "Visas några personuppgifter?",
      a: "Nej. Namn på ringmärkare och på dem som har rapporterat återfynd finns i materialet men visas inte på webbplatsen.",
    },
    {
      id: "mobil",
      q: "Fungerar sidan i mobilen?",
      a:
        "Ja. Menyer och filter lägger sig på rad efter varandra och breda tabeller går att skrolla i sidled. Diagrammen läses av genom att trycka på " +
        "eller dra över dem.",
    },
  );
  return items;
}
