# Recording Notes

## Market
- **Question**: LoL: Team Secret Whales vs MVK Esports (BO3) - LCP Regular Season
- **Slug**: lol-tsw-mvk-2026-02-06
- **Condition ID**: 0x8d4e0e3a293a62fde107403b27b390297c2c3dafb7d6d3d5c529d7ef2fffdf28
- **Sport Type**: moneyline
- **Game Start**: 2026-02-06 06:15:00+00

## Outcomes
  - Team Secret Whales: price=0.66, token_id=104990583506267861729734439680074288330079858431254201998930737514534645893163
  - MVK Esports: price=0.34, token_id=105881637809429992282816929913976739331553121434800963473247907613948348027949

## Market Stats (at recording start)
- **Total Volume**: 41255.053995
- **Liquidity**: 9792.892

## Recording Window
- **Start (UTC)**: 2026-02-06 06:16:17
- **End (UTC)**: 2026-02-06 06:21:20
- **Duration**: 302 seconds (5m 2s)
- **Sampling**: 5.0s interval

## Description
This market refers to the LoL match between Team Secret Whales and MVK Esports in the LCP Regular Season, scheduled for February 6 at 1:00AM ET.

This market will resolve to "Team Secret Whales" if Team Secret Whales win the match against MVK Esports.

This market will resolve to "MVK Esports" if MVK Esports win the match against Team Secret Whales.

If the match is canceled (not played at all), ends in a tie, or is delayed beyond 7 days from the scheduled date without a winner determined, this market will resolve to 50-50. 

If the match begins but is not completed, and one team wins due to the opponent's forfeiture, disqualification, or walkover, this market will resolve to the team who wins. 

If the match ends in a forfeit, disqualification, or walkover (team withdraws before the start and the other wins automatically), this market will resolve to 50-50. 

The resolution source for this market will be official information from https://liquipedia.net/leagueoflegends/Main_Page.

## Files
- `ob_data.db` — SQLite database
  - Table `pm_sports_market_1s`: per-second L1 + volume + spread + depth
  - Table `pm_sports_orderbook_top5_1s`: per-second Top5 bid/ask levels
