---
name: hft-binance-feed-arb
description: 'High-level context for the hft--binance-feed-arb-exp Rust project. Use when: describing the repo purpose, architecture, or contributing; answering questions about Binance depth stream fusion; checking for secrets before pushing to this open-source repo.'
---

# hft--binance-feed-arb-exp — Repo Context

## What This Repo Is

An **open-source Rust experiment** for fusing multiple Binance depth data streams into a single, high-resolution order book feed for HFT (high-frequency trading) backtesting and arbitrage research.

## The Problem

**Binance does not offer pure tick-by-tick Level-2 data.** All depth streams are conflated/aggregated:

- **`diffDepth` / `depthUpdate` streams** — Binance's primary depth streams aggregate multiple individual order-book updates into a single message. Even the now-removed `depth@0ms` stream (which was billed as "0ms aggregation") was still conflated — its BBO updates were measurably less frequent than the `bookTicker` stream.
- **`depth@0ms` no longer exists** — Binance has deprecated this stream. See: https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public
- **`bookTicker` stream** — Captures every BBO change individually (much higher update frequency), but only provides best bid/ask, not full depth.

To generate accurate fill simulations and realistic backtesting results, we must fuse these streams together.

## The Approach

This project follows the methodology described in the [hftbacktest "Fusing Depth Data" tutorial](https://hftbacktest.readthedocs.io/en/latest/tutorials/Fusing%20Depth%20Data.html):

1. **Ingest** multiple Binance depth feeds — candidates include **Partial Book Depth Streams**, **Individual Symbol Book Ticker Streams**, **Diff. Book Depth Streams**, and any others discovered to be useful based on experimental results
2. **Fuse** them into a single consolidated feed that preserves the highest possible update frequency and granularity
3. **Backtest** HFT/arbitrage strategies using the fused data

## ⚠️ Open Source Safety

This is an **open-source** repository. Before committing or pushing:

- **Never commit API keys, secret keys, or authentication credentials**
- Check for any hardcoded endpoints, tokens, or passwords
- Use environment variables or config files (gitignored) for any sensitive values
- Run `git diff --cached` before committing to review for accidental secrets
