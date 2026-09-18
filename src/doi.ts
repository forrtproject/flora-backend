/** Normalises a DOI to the bare `10.x/y` form used as the DynamoDB key. */
export function normDoi(s: string) {
  if (!s) return "";
  let x = s.trim().toLowerCase();
  for (const p of [
    "https://doi.org/",
    "http://doi.org/",
    "https://dx.doi.org/",
    "http://dx.doi.org/",
    "doi:",
  ]) {
    x = x.replace(p, "");
  }
  return x.trim();
}
