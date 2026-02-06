# Recording Notes

## Market
- **Question**: Warriors vs. Suns
- **Slug**: nba-gsw-phx-2026-02-05
- **Condition ID**: 0xc296b13aac16810c9daad88e6d0e82d1b69d3778aba5900134b190215cf8666d
- **Sport Type**: moneyline
- **Game Start**: 2026-02-06 03:00:00+00

## Outcomes
  - Warriors: price=0.9995, token_id=78323008020328440534445904698526900436573991706035782552200043416433638632347
  - Suns: price=0.0005, token_id=86922891867749820481201571899425107020583640132284146707793120383383493145737

## Market Stats (at recording start)
- **Total Volume**: 3612789.444557
- **Liquidity**: 1631785.26764

## Recording Window
- **Start (UTC)**: 2026-02-06 06:07:27
- **End (UTC)**: 2026-02-06 06:07:39
- **Duration**: 10 seconds (0m 10s)
- **Sampling**: 1 second interval

## Description
In the upcoming NBA game, scheduled for February 5 at 10:00PM ET:
If the Warriors win, the market will resolve to "Warriors".
If the Suns win, the market will resolve to "Suns".
If the game is postponed, this market will remain open until the game has been completed.
If the game is canceled entirely, with no make-up game, this market will resolve 50-50.
The result will be determined based on the final score including any overtime periods.

## Files
- `ob_data.db` — SQLite database
  - Table `pm_sports_market_1s`: per-second L1 + volume + spread + depth
  - Table `pm_sports_orderbook_top5_1s`: per-second Top5 bid/ask levels
