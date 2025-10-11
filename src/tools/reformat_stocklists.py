from pathlib import Path
import json

wrkdir = Path.cwd()

tick_jsons = wrkdir.glob("*tickers.json")

for tick_json in tick_jsons:
    print(tick_json.name)

    tks = json.loads(tick_json.read_text())
    print(len(tks))
    sorted_tks = sorted(tks, key=lambda x: x["added_date"], reverse=True)
    new_tks = {x["ticker"]: x for x in sorted_tks}

    new_path = tick_json.parent / f"{tick_json.stem}_new.json"
    new_path.write_text(json.dumps(new_tks, indent=4))
