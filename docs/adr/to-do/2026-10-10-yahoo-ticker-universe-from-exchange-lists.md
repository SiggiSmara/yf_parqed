# ADR 2026-10-10: Yahoo Ticker Universe from the Exchange Lists

## Status: To-Do

Agreed with the owner on 2026-10-10. Nothing is implemented. It starts after Step D of [ADR 2026-10-03: Daemon Resource Footprint](../in-progress/2026-10-03-daemon-resource-footprint.md) has been deployed and has held for a week; the order and the waits are in that ADR under "Rollout sequence and waits".

## Context

The Yahoo daemon decides which tickers to ask for from `tickers.json`. Tickers enter it from two downloaded lists (Nasdaq and NYSE), and until now they were meant to leave it through a rule of our own: no data on three days, a pause of 7 business days, one retry, then `permanently_dead`. That rule never ran in production, because the daemon did not save the registry. Step D of the 2026-10-03 ADR switches it off for good: every ticker is asked every night. This ADR decides who is in the registry.

While Step D was being decided, the owner asked whether the exchange list could say which tickers are active, instead of a rule of ours. It can, with three limits that the measurements below show. All of them were taken on 2026-10-10, read-only.

### 1. The list we download is a monthly copy

The lists come from datahub.io (`core/nasdaq-listings` and `core/nyse-other-listings`). The Nasdaq file ends with a line `File Creation Time: MMDDYYYYHH:MM`.

- The copy downloaded on 2026-10-04 and a download on 2026-10-10 both say 2026-09-30 21:31 and have the same size (268,844 bytes).
- That footer line is read as a ticker, so the registry holds one bogus entry per refresh: 14 entries named `File Creation Time: ...`, with stamps such as 04-30, 05-08, 05-29, 06-30 and 07-31. They show a refresh about once a month.
- Each bogus entry is asked for in every cycle and returns nothing.

A new listing can therefore wait a month before it is asked for. Yahoo serves 1-minute bars for 7 days, so its first weeks are lost.

The lists originate at Nasdaq Trader, which publishes them itself: `https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt` and `otherlisted.txt`. Both were reachable from the production host and both were created on 2026-10-09 at 21:31, the evening before.

| File | Rows | Columns |
|---|---|---|
| `nasdaqlisted.txt` | 5,630 | Symbol, Security Name, Market Category, Test Issue, Financial Status, Round Lot Size, ETF, NextShares |
| `otherlisted.txt` | 7,667 | ACT Symbol, Security Name, Exchange, CQS Symbol, ETF, Round Lot Size, Test Issue, NASDAQ Symbol |

`otherlisted.txt` covers several exchanges: NYSE (`N`) 2,912 rows, NYSE Arca (`P`) 2,756, Cboe (`Z`) 1,661, NYSE American (`A`) 314, others 24. The datahub NYSE file is essentially the `N` rows: 2,898 of its 2,916 symbols are `N` rows in the exchange file. 37 rows across both files are marked as test issues.

### 2. The list and Yahoo disagree on about 700 tickers

Compared: the 9,263 real registry entries, the Nasdaq and NYSE rows of the files of 2026-10-09, and whether a ticker has a stored file for October 2026.

| Registry tickers | Count | If the list alone decided |
|---|---|---|
| Not listed, no October data | 902 | Dropped, correctly |
| Not listed, with October bars | 76 | No longer collected, although Yahoo serves them |
| Listed, no October data | 624 | Asked every night, returning nothing |

- **The 76.** Measured against the older list, 43 unlisted tickers had October bars, and 39 of them traded on 8 or 9 October (LAZR has 1,293 bars in October). Why they are unlisted was not looked into.
- **The 624.** Measured against the older list (637 then): 353 contain `$`, 109 contain a dot and 175 are plain. None of the 353 `$` symbols has October data, and 1 of the 109 dotted ones has. The plain ones are mostly SPAC units, warrants and rights.
- 250 listed symbols are not in the registry. Most are warrants, units and rights that the existing filter excludes; 36 are not in the older lists at all, and some of those are test symbols.

### 3. Yahoo spells some symbols differently

The exchange file gives three spellings of a symbol and none of them is Yahoo's. Yahoo was asked for 7 days of 1-minute bars on 2026-10-10:

| Instrument | Exchange spellings | Bars | Yahoo's spelling | Bars |
|---|---|---|---|---|
| JPMorgan preferred L | `JPM$L`, `JPMpL`, `JPM-L` | 0, 0, 0 | `JPM-PL` | 1,300 |
| Berkshire Hathaway B | `BRK.B` | 0 | `BRK-B` | 2,730 |
| Brown-Forman B | `BF.B` | 0 | `BF-B` | 2,653 |
| Andina A | `AKO.A` | 0 | `AKO-A` | 4 |
| KeyCorp preferred I | `KEY$I` | not asked | `KEY-PI` | 531 |

Berkshire Hathaway B and Brown-Forman B have no stored directory: they have never been collected.

In the datahub file a dot can mean a share class, a unit, a warrant or a right, and the existing filter guesses from the suffix and the name. The exchange file's `NASDAQ Symbol` column tells them apart. Of the 2,912 NYSE symbols, 507 are not plain letters:

| Kind | Count | ACT Symbol | NASDAQ Symbol |
|---|---|---|---|
| Preferred shares | 358 | `XXX$A` (4 without a letter: `XXX$`) | `XXX-A` |
| Warrants | 49 | `XXX.W`, `XXX.WS` | `XXX+` |
| Units | 47 | `XXX.U` | `XXX=` |
| Share classes | 33 | `XXX.A` | `XXX.A` |
| Rights | 20 | `XXX.R` | `XXX^` |

The Nasdaq file has one such symbol (`ZXYZ.A`). Of the 8,542 Nasdaq and NYSE symbols, 8,034 (94.1%) are plain letters and need no rule, 392 (4.6%) are preferred shares and share classes that need a rename to be collected, and 116 (1.4%) are warrants, units and rights, which are excluded on purpose.

## Decision

### 1. Read the exchange's own lists, every day

- **Source.** `nasdaqlisted.txt` and `otherlisted.txt` from Nasdaq Trader replace the two datahub files.
- **Universe.** Every row of the Nasdaq file and the `N` rows of the other file, without test issues (`Test Issue` = `Y`). That is today's universe. The other exchanges are not added.
- **Daily.** The unit template gets `--ticker-maintenance daily` in place of `weekly`.
- **The footer is not a ticker.** The `File Creation Time` line is read as the file's date and never as a symbol. The 14 bogus registry entries are removed.
- **A bad download changes nothing.** A list is used only when the request succeeded, the footer is present and the number of symbols has not fallen sharply against the previous list. Otherwise the previous list stays in use and an error is logged. Today the download is not checked and overwrites the previous file. A list whose creation stamp is more than a few days old is logged as a warning, which is how the monthly copy would have been noticed.

### 2. The list decides who is asked, with one rule of our own

- **A listed ticker is asked every night.** Nothing else is considered.
- **An unlisted ticker is asked while it delivers bars.** It is removed from the registry when it is unlisted and has delivered no bar for 30 days. Its stored files stay. If it appears on a list again, it is added like a new ticker.
- **Who would be removed is logged first.** From the first day the daemon logs which tickers are unlisted and since when, so the list can be read before the rule removes anyone, which is 30 days after the deploy at the earliest.
- **Manual tickers are exempt**, as they are today: `add-ticker` and `remove-ticker` are not changed.
- **Warrants, units and rights stay excluded.** For NYSE rows they are recognised by the `NASDAQ Symbol` column (`+`, `=`, `^`), not by guessing from the suffix. The Nasdaq file has no such column, so its rows keep the existing name filter.

### 3. Preferred shares and share classes are fetched under Yahoo's spelling

- **Two rules.** A preferred share `XXX$A` becomes `XXX-PA`. A share class `XXX.A` becomes `XXX-A`. The preferred rule was checked on two symbols and the class rule on three; all of them are to be checked against Yahoo before the rules are switched on.
- **Storage uses Yahoo's spelling.** The directory is `ticker=BRK-B` and the registry entry is `BRK-B`. The exchange symbol is recorded in the entry. Decided by the owner: it matches what Yahoo serves and every existing directory, and it keeps `$` out of paths.
- **The old entries go.** The registry holds the exchange spellings of these instruments today (`JPM$L`, `BRK.B`). They have never returned data. Each is removed when its Yahoo-spelled entry is added. The one dotted symbol that has October data is looked at first.
- **This widens what is collected** by about 390 instruments, so it is deployed last and on its own.

### 4. Not part of this ADR

- **A stable identity for an instrument.** The exchange files carry symbols only, with no ISIN or CUSIP. When a company changes its ticker, one symbol leaves the list and another arrives, and nothing links them. Linking them is the problem the ISIN mapping service was meant to solve for Xetra. The owner decided on 2026-10-10 that this is a large piece of work for later.
- **Other exchanges.** NYSE Arca (2,756 symbols, mostly ETFs), Cboe (1,661) and NYSE American (314) are in the same file and are not collected.
- **Pre-market and after-hours bars.** Yahoo has them; the fetcher does not ask for them (`prepost` is off). In September and October 2026 every stored bar of eight liquid tickers falls between 09:30 and 15:59 exchange time.

## Sequenced Steps

Each step is one deploy. Step 2 follows Step 1 after the wait given in the 2026-10-03 ADR.

- [ ] **Step 1** — The list source and the rule for who is asked (Decisions 1 and 2).
  - [ ] Download and parse the two exchange files; universe filter; test issues excluded; footer read as the date.
  - [ ] Download check: status, footer, symbol count against the previous list; previous list kept on failure; warning for an old stamp.
  - [ ] Remove the 14 bogus entries (one-off, with the owner's go-ahead: it edits `tickers.json`).
  - [ ] Unlisted tickers: logged from the first day; removed after 30 days without a bar. Needs the date of the newest stored bar, which Step D of the 2026-10-03 ADR saves.
  - [ ] Warrants, units and rights by the `NASDAQ Symbol` column for NYSE rows.
  - [ ] `--ticker-maintenance daily` in `daemon-manage.sh` and `docs/daemon/INSTALLATION.md`. When deploying, answer **y** to the template question.
  - [ ] Tests: a failed, truncated or empty download leaves the registry and the previous list untouched; the footer never becomes a ticker; a listed ticker without data stays; an unlisted ticker with bars stays; an unlisted ticker is removed only after 30 days without a bar; a ticker that returns to the list is asked again; test issues and other exchanges are not added.
- [ ] **Step 2** — Yahoo spelling for preferred shares and share classes (Decision 3).
  - [ ] Dry run: every translated symbol and whether Yahoo returns bars for it; the owner reads the list before the rules are switched on.
  - [ ] The two rules; the exchange symbol recorded in the registry entry; the old exchange-spelled entries removed.
  - [ ] Tests: both rules; a warrant, unit or right is never translated; an entry with stored data is never removed.

Run `uv run pytest` after each step; all tests must pass before moving on.

## Risk Controls

- **The list can be wrong, so it never removes a ticker by itself.** A ticker leaves only when it is unlisted and has been silent for 30 days. A broken list therefore cannot stop a ticker that trades, and for 30 days after the deploy the rule removes nobody.
- **A ticker that is removed stops being collected for good**, unless it returns to a list or is added by hand. Thin unlisted tickers exist (BTA has 7 bars in October 2026), which is why the limit is 30 days and not Yahoo's 7.
- **At the deploy, the 902 unlisted and silent tickers are still asked for 30 nights.** That is about a tenth of a cycle. They can be removed earlier with `tools/prune_registry.py` if the owner wants.
- **The rename rules are checked on five symbols.** The dry run in Step 2 is the check on all of them. A symbol the rules get wrong returns nothing and loses nothing: it was not collected before either.
- **Removing registry entries edits `tickers.json`.** No Parquet file is deleted or renamed by anything in this ADR.
- **Two sources of truth for a name.** After Step 2 the exchange calls an instrument `BRK.B` and the registry and the disk call it `BRK-B`. The registry entry records both. Anything that compares the registry with the list has to translate first.

## Alternatives Considered

**The list alone decides.** Simpler: no rule of our own at all. Rejected because it stops collecting the 76 unlisted tickers that still deliver bars, and because a broken download would unlist everything.

**Keep the datahub files and refresh them daily.** Rejected: the Nasdaq file there changes about once a month, so a daily refresh downloads the same file.

**Store renamed instruments under the exchange spelling.** One name per instrument across list and disk. Rejected by the owner: the data comes from Yahoo, every existing directory is already in Yahoo's spelling, and `JPM$L` is awkward in a path.

**Keep the not-found rule (3 days, pause, dead) with an outage guard.** Rejected in the 2026-10-03 ADR, Decision 6: the pause is longer than Yahoo's 7 days, and the guard recognises only the outages it was written for.

## Consequences

- New listings are asked for within a day of appearing on the exchange's list.
- A ticker leaves the registry because the exchange dropped it and Yahoo has gone silent on it, not because of a count of empty answers.
- About 390 instruments that were never collected, Berkshire Hathaway B among them, are collected from the day Step 2 is deployed. Their earlier history is not available at 1-minute resolution.
- The registry key is Yahoo's symbol. For 94% of the instruments that is also the exchange's symbol.
- Ticker changes still split an instrument's history across two directories. That waits for the identity work.
