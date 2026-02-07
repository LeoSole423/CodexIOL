import argparse
import json
import sqlite3
import subprocess


def value_of(qty: float, price: float, asset_type: str) -> float:
    # In IOL portfolio, Argentine public bonds are priced per 100 nominal.
    if asset_type == "TitulosPublicos":
        return qty * price / 100.0
    return qty * price


def qty_from_value(target_value: float, price: float, asset_type: str) -> float:
    if asset_type == "TitulosPublicos":
        return target_value * 100.0 / price
    return target_value / price


def round_qty(raw_qty: float, asset_type: str) -> float:
    if asset_type in ("TitulosPublicos", "CEDEARS", "ACCIONES"):
        return float(int(round(raw_qty)))
    # FCI supports fractional shares.
    return float(raw_qty)


def fmt_qty(qty: float, asset_type: str) -> str:
    if asset_type in ("TitulosPublicos", "CEDEARS", "ACCIONES"):
        return str(int(round(qty)))
    return f"{qty:.4f}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/iol_history.db")
    ap.add_argument("--liquidity-symbol", default="ADRDOLA", help="FCI symbol for the opportunity cash bucket")
    ap.add_argument("--btc-symbol", default="IBIT", help="BTC proxy symbol (CEDEAR), e.g. IBIT")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    snap = conn.execute(
        "SELECT snapshot_date, total_value FROM portfolio_snapshots ORDER BY snapshot_date DESC LIMIT 1"
    ).fetchone()
    if not snap:
        raise SystemExit("No snapshot found in DB.")
    snapshot_date = snap["snapshot_date"]
    portfolio_total = float(snap["total_value"] or 0.0)

    assets = [
        dict(r)
        for r in conn.execute(
            """
            SELECT symbol, description, type, quantity, last_price, total_value
            FROM portfolio_assets
            WHERE snapshot_date = ?
            """,
            (snapshot_date,),
        ).fetchall()
    ]
    conn.close()

    # Targets: moderate with 10% for opportunities, where BTC is 3% of that 10%
    # bucket (0.3% of total). BTC exposure is implemented using a BTC-proxy CEDEAR
    # (default: IBIT). The remaining 9.7% sits in a "cash-like" FCI (default: ADRDOLA).
    target_pct = {
        args.btc_symbol: 0.3,
        args.liquidity_symbol: 9.7,
        "GLD": 12.0,
        "SPY": 32.0,
        "BRKB": 5.0,
        "DIA": 3.0,
        "EWZ": 2.0,
        "MSFT": 1.0,
        "NVDA": 1.0,
        "BABA": 1.0,
        "AL30": 7.0,
        "TX26": 19.0,
        "YPFD": 3.0,
        "PAMP": 2.0,
        "IRSA": 1.0,
        "GGAL": 0.5,
        "ALUA": 0.5,
    }

    by_sym = {a["symbol"]: a for a in assets}
    # Everything currently held but not in targets is implicitly set to 0% (sell all).
    for sym in list(by_sym.keys()):
        target_pct.setdefault(sym, 0.0)

    # Fetch missing prices for symbols we want to buy but don't currently hold.
    for sym in list(target_pct.keys()):
        if sym in by_sym:
            continue
        q = subprocess.run(
            ["iol", "market", "quote", "--market", "bcba", "--symbol", sym],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        quote = json.loads(q.stdout)
        price = float(quote.get("ultimoPrecio") or 0.0)
        inferred_type = "CEDEARS"
        if sym == args.liquidity_symbol:
            inferred_type = "FondoComundeInversion"
        by_sym[sym] = {
            "symbol": sym,
            "description": quote.get("descripcionTitulo") or sym,
            "type": inferred_type,
            "quantity": 0.0,
            "last_price": price,
            "total_value": 0.0,
        }

    plan = []
    for sym, pct in target_pct.items():
        a = by_sym[sym]
        asset_type = a["type"]
        cur_qty = float(a["quantity"] or 0.0)
        price = float(a["last_price"] or 0.0)
        cur_val = float(a["total_value"] or 0.0)

        tgt_val_raw = portfolio_total * pct / 100.0
        raw_qty = qty_from_value(tgt_val_raw, price, asset_type)
        tgt_qty = round_qty(raw_qty, asset_type)
        if pct > 0 and asset_type in ("TitulosPublicos", "CEDEARS", "ACCIONES") and raw_qty > 0 and tgt_qty == 0:
            tgt_qty = 1.0
        tgt_val = value_of(tgt_qty, price, asset_type)

        plan.append(
            {
                "symbol": sym,
                "type": asset_type,
                "price": price,
                "cur_qty": cur_qty,
                "cur_val": cur_val,
                "cur_pct": (cur_val / portfolio_total * 100.0) if portfolio_total else 0.0,
                "tgt_pct": pct,
                "tgt_qty": tgt_qty,
                "tgt_val": tgt_val,
            }
        )

    # Absorb rounding drift into the liquidity FCI (fractional), keeping other targets integer-rounded.
    total_target = sum(p["tgt_val"] for p in plan)
    drift = portfolio_total - total_target
    absorber = next((p for p in plan if p["symbol"] == args.liquidity_symbol), None)
    if absorber is None:
        raise SystemExit(f"Missing liquidity symbol in plan: {args.liquidity_symbol}")
    absorber["tgt_val"] += drift
    absorber["tgt_qty"] = absorber["tgt_val"] / absorber["price"]

    # Sort by current value (largest first).
    plan.sort(key=lambda r: r["cur_val"], reverse=True)

    print(f"snapshot_date\t{snapshot_date}")
    print(f"portfolio_total_ars\t{portfolio_total:.2f}")
    print("note\tBonds (TitulosPublicos) are priced per 100 nominal in this data (value = qty*price/100).")
    print("note\tFCI subscribe is usually by amount; qty here is 'cuotapartes' estimate based on last price.")
    print("---")

    headers = [
        "symbol",
        "type",
        "price_ars",
        "cur_qty",
        "cur_value_ars",
        "cur_pct",
        "tgt_pct",
        "tgt_qty",
        "tgt_value_ars",
        "delta_value_ars",
        "delta_qty",
        "action",
    ]
    print("\t".join(headers))
    for p in plan:
        delta = p["tgt_qty"] - p["cur_qty"]
        if delta > 1e-9:
            action = "BUY"
        elif delta < -1e-9:
            action = "SELL"
        else:
            action = "HOLD"

        print(
            "\t".join(
                [
                    p["symbol"],
                    p["type"],
                    f"{p['price']:.6f}".rstrip("0").rstrip("."),
                    fmt_qty(p["cur_qty"], p["type"]),
                    f"{p['cur_val']:.2f}",
                    f"{p['cur_pct']:.2f}",
                    f"{p['tgt_pct']:.2f}",
                    fmt_qty(p["tgt_qty"], p["type"]),
                    f"{p['tgt_val']:.2f}",
                    f"{(p['tgt_val'] - p['cur_val']):.2f}",
                    fmt_qty(delta, p["type"]),
                    action,
                ]
            )
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
