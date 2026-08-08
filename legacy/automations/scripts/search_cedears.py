import argparse
import json
import re
import subprocess


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("pattern", help="Regex to match against 'simbolo descripcion' (case-insensitive)")
    args = ap.parse_args()

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

    rx = re.compile(args.pattern, re.IGNORECASE)
    out = []
    for x in data:
        s = f"{x.get('simbolo','')} {x.get('descripcion','')}"
        if rx.search(s):
            out.append(
                {
                    "simbolo": x.get("simbolo"),
                    "descripcion": x.get("descripcion"),
                    "ultimoPrecio": x.get("ultimoPrecio"),
                    "variacionPorcentual": x.get("variacionPorcentual"),
                }
            )
    out.sort(key=lambda m: (m.get("simbolo") or ""))
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

