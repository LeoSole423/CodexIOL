import json
import re
import subprocess


PATTERNS = [
    r"\bBTC\b",
    r"bitcoin",
    r"\bIBIT\b",
    r"\bFBTC\b",
    r"\bARKB\b",
    r"\bBITO\b",
    r"\bGBTC\b",
    r"\bMSTR\b",
    r"\bCOIN\b",
]


def main() -> int:
    proc = subprocess.run(
        [
            "iol",
            "market",
            "panel-quotes",
            "--instrument",
            "Acciones",
            "--panel",
            "CEDEARs",
            "--country",
            "argentina",
        ],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    payload = json.loads(proc.stdout)
    data = payload.get("titulos") if isinstance(payload, dict) else payload
    if not isinstance(data, list):
        raise SystemExit(f"Unexpected payload shape: {type(payload).__name__}")
    rx = re.compile("|".join(f"(?:{p})" for p in PATTERNS), re.IGNORECASE)
    matches = []
    for x in data:
        s = f"{x.get('simbolo','')} {x.get('descripcion','')}"
        if rx.search(s):
            matches.append(
                {
                    "simbolo": x.get("simbolo"),
                    "descripcion": x.get("descripcion"),
                    "ultimoPrecio": x.get("ultimoPrecio"),
                    "variacionPorcentual": x.get("variacionPorcentual"),
                    "mercado": x.get("mercado"),
                }
            )
    matches.sort(key=lambda m: (m.get("simbolo") or ""))
    print(json.dumps(matches, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
