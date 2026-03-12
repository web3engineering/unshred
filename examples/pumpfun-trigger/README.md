# pumpfun-trigger

Low-latency trigger example built on `unshred`:

- scans reconstructed transactions for Pump `create` (excludes `create_v2`)
- verifies the created mint equals your expected mint
- keeps Falcon sender connection warm
- continuously caches finalized blockhashes for hot-path use
- on trigger, derives creator vault from `create` instruction, signs, and sends tx:
  `CU price`, `CU limit`, `create_ata`, `buy_exact_quote`, optional Falcon tip transfer
- in non-`dry-run` mode, also appends a SOL tip transfer to
  `Fa1con1i7mpa7Qc6epYJ6r4P9AbU77DFFz173r59Df1x` using `--tip-sol`
- logs trigger latency (`handler_to_send_us`)
- logs search-efficiency snapshot every 10,000 processed transactions

## Notes

- In non-`dry-run` mode transactions are submitted to `--sender-url` via `sendTransaction`.

## Run

```bash
cargo run --release -- \
  --bind-address 0.0.0.0:8001 \
  --num-fec-workers 4 \
  --num-batch-workers 2 \
  --expected-mint <MINT_PUBKEY> \
  --rpc-url https://api.mainnet-beta.solana.com \
  --signer-keypair /path/to/keypair.json \
  --sender-url "http://fra.falcon.wtf/?api-key=a28715bc-3638-4794-8332-ee1c04c3103d" \
  --tip-sol 0.001
```

## Compile-Time Debug Features

- `dry-run`: do not send to sender endpoint on trigger (logs simulated send instead).
- `no-filter`: bypass Pump/mint filtering and treat every transaction as a trigger candidate.

Debug run on your current shred socket:

```bash
cargo run --release --features "dry-run,no-filter" -- \
  --bind-address 0.0.0.0:28000 \
  --num-fec-workers 4 \
  --num-batch-workers 2 \
  --expected-mint 11111111111111111111111111111111 \
  --rpc-url https://api.mainnet-beta.solana.com \
  --signer-keypair /path/to/keypair.json
```

With `dry-run`, `--tip-sol` has no effect.
