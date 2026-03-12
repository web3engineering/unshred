use std::str::FromStr;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
#[cfg(not(feature = "dry-run"))]
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use clap::Parser;
use reqwest::Client;
#[cfg(not(feature = "dry-run"))]
use serde_json::json;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::hash::Hash;
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::message::Message;
use solana_sdk::native_token::LAMPORTS_PER_SOL;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{read_keypair_file, Keypair, Signature, Signer};
use solana_sdk::system_instruction;
use solana_sdk::system_program;
use solana_sdk::transaction::Transaction;
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};
use unshred::{TransactionEvent, TransactionHandler, UnshredProcessor};

const DEFAULT_PUMP_PROGRAM_ID: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const CREATE_DISCRIMINATOR: [u8; 8] = [24, 30, 200, 40, 5, 28, 7, 119];
const CREATE_CREATOR_ACCOUNT_INDEX: usize = 7;
const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const ASSOCIATED_TOKEN_PROGRAM_ID: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";
const PUMP_EVENT_AUTHORITY: &str = "Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1";
const PUMP_FEE_RECIPIENT: &str = "62qc2CNXwrYqQScmEdiZFFAnJR262PxWEuNQtxfafNgV";
const PUMP_FEE_CONFIG_ACCOUNT: &str = "8Wf5TiAheLUqBrKXeYg2JtAFFMWtKdG2BSFgqUcPVwTt";
const PUMP_FEE_PROGRAM: &str = "pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ";
const BUY_EXACT_SOL_IN_DISCRIMINATOR: [u8; 8] = [56, 252, 116, 8, 158, 223, 205, 95];
const FALCON_TIP_ACCOUNT: &str = "Fa1con1i7mpa7Qc6epYJ6r4P9AbU77DFFz173r59Df1x";
const DEFAULT_SENDER_URL: &str =
    "http://fra.falcon.wtf/?api-key=a28715bc-3638-4794-8332-ee1c04c3103d";

#[derive(Parser, Debug, Clone)]
struct Args {
    #[arg(long, default_value = "0.0.0.0:8001")]
    bind_address: String,

    #[arg(long, default_value_t = 4)]
    num_fec_workers: u8,

    #[arg(long, default_value_t = 2)]
    num_batch_workers: u8,

    #[arg(long)]
    expected_mint: Pubkey,

    #[arg(long, default_value = DEFAULT_PUMP_PROGRAM_ID)]
    pump_program_id: Pubkey,

    #[arg(long)]
    rpc_url: String,

    #[arg(
        long,
        help = "Path to signer keypair file used to sign locally-built transactions"
    )]
    signer_keypair: Option<String>,

    #[arg(long, default_value_t = 200_000)]
    compute_unit_limit: u32,

    #[arg(long, default_value_t = 1_000_000)]
    compute_unit_price_micro_lamports: u64,

    #[arg(long, default_value_t = 1_000_000)]
    buy_exact_sol_amount: u64,

    #[arg(long, default_value_t = 0)]
    buy_exact_min_token_amount: u64,

    #[arg(long, default_value = DEFAULT_SENDER_URL)]
    sender_url: String,

    #[arg(long, default_value_t = 0.0)]
    tip_sol: f64,
}

#[derive(Debug)]
struct TriggerEvent {
    slot: u64,
    triggering_signature: Signature,
    extracted_mint: Pubkey,
    extracted_creator: Option<Pubkey>,
    received_at: Instant,
}

struct PumpCreateHandler {
    #[cfg_attr(feature = "no-filter", allow(dead_code))]
    expected_mint: Pubkey,
    pump_program: Pubkey,
    fired: Arc<AtomicBool>,
    trigger_tx: mpsc::Sender<TriggerEvent>,
    metrics: Arc<SearchMetrics>,
}

impl PumpCreateHandler {
    fn classify_transaction(&self, event: &TransactionEvent) -> Classification {
        let keys = event.transaction.message.static_account_keys();
        let mut saw_pump_program = false;
        let mut saw_pump_create = false;
        let mut first_create_mint: Option<Pubkey> = None;
        let mut first_create_creator: Option<Pubkey> = None;

        for ix in event.transaction.message.instructions() {
            let Some(program_id) = keys.get(ix.program_id_index as usize) else {
                continue;
            };

            if *program_id != self.pump_program {
                continue;
            }
            saw_pump_program = true;

            if ix.data.len() < 8 {
                continue;
            }

            let discriminator: [u8; 8] = ix.data[0..8].try_into().expect("len checked");
            if discriminator != CREATE_DISCRIMINATOR {
                continue;
            }
            saw_pump_create = true;

            let Some(first_account_idx) = ix.accounts.first() else {
                continue;
            };
            let Some(mint) = keys.get(*first_account_idx as usize) else {
                continue;
            };
            let creator = ix
                .accounts
                .get(CREATE_CREATOR_ACCOUNT_INDEX)
                .and_then(|idx| keys.get(*idx as usize))
                .copied();
            if first_create_mint.is_none() {
                first_create_mint = Some(*mint);
            }
            if first_create_creator.is_none() {
                first_create_creator = creator;
            }

            #[cfg(feature = "no-filter")]
            {
                return Classification {
                    detection: Detection::TargetMatch,
                    extracted_mint: Some(*mint),
                    extracted_creator: creator,
                };
            }
            #[cfg(not(feature = "no-filter"))]
            if *mint == self.expected_mint {
                return Classification {
                    detection: Detection::TargetMatch,
                    extracted_mint: Some(*mint),
                    extracted_creator: creator,
                };
            }
        }

        if saw_pump_create {
            Classification {
                detection: Detection::PumpCreateOtherMint,
                extracted_mint: first_create_mint,
                extracted_creator: first_create_creator,
            }
        } else if saw_pump_program {
            Classification {
                detection: Detection::PumpProgramOnly,
                extracted_mint: None,
                extracted_creator: None,
            }
        } else {
            Classification {
                detection: Detection::NotPump,
                extracted_mint: None,
                extracted_creator: None,
            }
        }
    }
}

impl TransactionHandler for PumpCreateHandler {
    fn handle_transaction(&self, event: &TransactionEvent) -> Result<()> {
        let handler_received_at = Instant::now();
        let classify_started_at = Instant::now();
        let classification = self.classify_transaction(event);
        let classify_elapsed_ns = classify_started_at.elapsed().as_nanos() as u64;
        self.metrics
            .record(classification.detection, classify_elapsed_ns);

        if !matches!(classification.detection, Detection::TargetMatch) {
            return Ok(());
        }
        let triggering_signature = event
            .transaction
            .signatures
            .first()
            .copied()
            .unwrap_or_default();
        let extracted_mint = classification
            .extracted_mint
            .ok_or_else(|| anyhow!("target match missing extracted mint"))?;
        info!(
            slot = event.slot,
            triggering_signature = %triggering_signature,
            extracted_mint = %extracted_mint,
            "creation match found"
        );

        if self
            .fired
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Ok(());
        }

        if let Err(e) = self.trigger_tx.try_send(TriggerEvent {
            slot: event.slot,
            triggering_signature,
            extracted_mint,
            extracted_creator: classification.extracted_creator,
            received_at: handler_received_at,
        }) {
            self.fired.store(false, Ordering::SeqCst);
            return Err(anyhow!("trigger channel send failed: {e}"));
        }

        Ok(())
    }
}

#[derive(Copy, Clone)]
enum Detection {
    NotPump,
    PumpProgramOnly,
    PumpCreateOtherMint,
    TargetMatch,
}

#[derive(Copy, Clone)]
struct Classification {
    detection: Detection,
    extracted_mint: Option<Pubkey>,
    extracted_creator: Option<Pubkey>,
}

struct SearchMetrics {
    processed: AtomicU64,
    pump_program_seen: AtomicU64,
    pump_create_seen: AtomicU64,
    target_match_seen: AtomicU64,
    create_search_ns_total: AtomicU64,
    next_log_at: AtomicU64,
}

impl SearchMetrics {
    fn new() -> Self {
        Self {
            processed: AtomicU64::new(0),
            pump_program_seen: AtomicU64::new(0),
            pump_create_seen: AtomicU64::new(0),
            target_match_seen: AtomicU64::new(0),
            create_search_ns_total: AtomicU64::new(0),
            next_log_at: AtomicU64::new(10_000),
        }
    }

    fn record(&self, detection: Detection, create_search_ns: u64) {
        let processed = self.processed.fetch_add(1, Ordering::Relaxed) + 1;
        self.create_search_ns_total
            .fetch_add(create_search_ns, Ordering::Relaxed);
        match detection {
            Detection::NotPump => {}
            Detection::PumpProgramOnly => {
                self.pump_program_seen.fetch_add(1, Ordering::Relaxed);
            }
            Detection::PumpCreateOtherMint => {
                self.pump_program_seen.fetch_add(1, Ordering::Relaxed);
                self.pump_create_seen.fetch_add(1, Ordering::Relaxed);
            }
            Detection::TargetMatch => {
                self.pump_program_seen.fetch_add(1, Ordering::Relaxed);
                self.pump_create_seen.fetch_add(1, Ordering::Relaxed);
                self.target_match_seen.fetch_add(1, Ordering::Relaxed);
            }
        }

        let target = self.next_log_at.load(Ordering::Relaxed);
        if processed < target {
            return;
        }
        if self
            .next_log_at
            .compare_exchange(
                target,
                target.saturating_add(10_000),
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
        {
            return;
        }

        let pump = self.pump_program_seen.load(Ordering::Relaxed);
        let create = self.pump_create_seen.load(Ordering::Relaxed);
        let target_matches = self.target_match_seen.load(Ordering::Relaxed);
        let create_search_ns_total = self.create_search_ns_total.load(Ordering::Relaxed);
        let avg_create_search_ns = create_search_ns_total / processed;
        let avg_create_search_us = avg_create_search_ns as f64 / 1_000.0;
        let pump_ratio = (pump as f64 / processed as f64) * 100.0;
        let create_ratio = (create as f64 / processed as f64) * 100.0;
        let hit_ratio = (target_matches as f64 / processed as f64) * 100.0;

        info!(
            processed,
            create_search_ns_total,
            avg_create_search_ns,
            avg_create_search_us = format_args!("{avg_create_search_us:.3}"),
            pump_program_seen = pump,
            pump_create_seen = create,
            target_match_seen = target_matches,
            pump_ratio_pct = format_args!("{pump_ratio:.4}"),
            create_ratio_pct = format_args!("{create_ratio:.4}"),
            hit_ratio_pct = format_args!("{hit_ratio:.6}"),
            "creation-search efficiency snapshot"
        );
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();

    let (trigger_tx, trigger_rx) = mpsc::channel::<TriggerEvent>(1);
    let (blockhash_tx, blockhash_rx) = watch::channel::<Option<Hash>>(None);
    let signer_path = args
        .signer_keypair
        .as_ref()
        .ok_or_else(|| anyhow!("--signer-keypair is required"))?;
    let signer = read_keypair_file(signer_path)
        .map_err(|e| anyhow!("failed reading keypair file `{signer_path}`: {e}"))?;
    let tip_lamports = tip_sol_to_lamports(args.tip_sol)?;
    let base_addresses =
        derive_pump_trade_addresses(args.pump_program_id, args.expected_mint, None);
    let http = Client::builder()
        .tcp_nodelay(true)
        .pool_max_idle_per_host(1)
        .pool_idle_timeout(Duration::from_secs(300))
        .build()
        .context("failed building reqwest client")?;

    #[cfg(not(feature = "dry-run"))]
    tokio::spawn(run_sender_keepalive(
        http.clone(),
        args.sender_url.clone(),
        Duration::from_secs(2),
    ));

    tokio::spawn({
        let rpc_url = args.rpc_url.clone();
        let bh_tx = blockhash_tx.clone();
        async move {
            if let Err(e) = run_blockhash_loop(rpc_url, bh_tx).await {
                warn!("blockhash loop failed: {e}");
            }
        }
    });

    tokio::spawn(run_sender(
        http,
        args.clone(),
        signer,
        tip_lamports,
        base_addresses,
        blockhash_rx,
        trigger_rx,
    ));

    let handler = PumpCreateHandler {
        expected_mint: args.expected_mint,
        pump_program: args.pump_program_id,
        fired: Arc::new(AtomicBool::new(false)),
        trigger_tx,
        metrics: Arc::new(SearchMetrics::new()),
    };

    info!(expected_mint = %args.expected_mint, pump_program = %args.pump_program_id, "starting pumpfun trigger");

    #[cfg(feature = "no-filter")]
    info!("feature `no-filter` enabled: mint check disabled; still requires pump `create` instruction (not create_v2)");

    #[cfg(feature = "dry-run")]
    info!("feature `dry-run` enabled: sender sends are simulated");

    let processor = UnshredProcessor::builder()
        .handler(handler)
        .bind_address(args.bind_address)
        .num_fec_workers(args.num_fec_workers)
        .num_batch_workers(args.num_batch_workers)
        .build()?;

    processor.run().await
}

async fn run_sender(
    http: Client,
    args: Args,
    signer: Keypair,
    tip_lamports: u64,
    base_addresses: PumpTradeAddresses,
    blockhash_rx: watch::Receiver<Option<Hash>>,
    mut trigger_rx: mpsc::Receiver<TriggerEvent>,
) {
    info!(
        signer = %signer.pubkey(),
        mint = %args.expected_mint,
        pump_program = %args.pump_program_id,
        tip_sol = args.tip_sol,
        tip_lamports,
        "tx template ready; waiting for create triggers"
    );

    while let Some(trigger) = trigger_rx.recv().await {
        info!(
            slot = trigger.slot,
            triggering_signature = %trigger.triggering_signature,
            extracted_mint = %trigger.extracted_mint,
            extracted_creator = ?trigger.extracted_creator,
            handler_to_send_us = trigger.received_at.elapsed().as_micros() as u64,
            "trigger matched"
        );
        let Some(creator) = trigger.extracted_creator else {
            warn!("trigger matched but creator account was not extracted");
            continue;
        };
        let Some(blockhash) = *blockhash_rx.borrow() else {
            warn!("trigger matched but no finalized blockhash is cached yet");
            continue;
        };
        #[cfg(feature = "no-filter")]
        let (tx_mint, addresses) = {
            let tx_mint = trigger.extracted_mint;
            let addresses =
                derive_pump_trade_addresses(args.pump_program_id, tx_mint, Some(creator));
            (tx_mint, addresses)
        };
        #[cfg(not(feature = "no-filter"))]
        let (tx_mint, addresses) = {
            let mut addresses = base_addresses;
            addresses.creator_vault = Pubkey::find_program_address(
                &[b"creator-vault", creator.as_ref()],
                &args.pump_program_id,
            )
            .0;
            (args.expected_mint, addresses)
        };

        match build_signed_trade_tx(
            blockhash,
            &signer,
            args.pump_program_id,
            tx_mint,
            addresses,
            args.compute_unit_limit,
            args.compute_unit_price_micro_lamports,
            args.buy_exact_sol_amount,
            args.buy_exact_min_token_amount,
            tip_lamports,
        ) {
            Ok(tx) => {
                let local_sig = tx.signatures.first().copied().unwrap_or_default();
                #[cfg(not(feature = "dry-run"))]
                match send_signed_transaction(&http, &args.sender_url, &tx).await {
                    Ok(remote_sig) => {
                        info!(
                            slot = trigger.slot,
                            mint = %tx_mint,
                            creator = %creator,
                            creator_vault = %addresses.creator_vault,
                            blockhash = %blockhash,
                            local_sig = %local_sig,
                            remote_sig = %remote_sig,
                            trigger_to_send_us = trigger.received_at.elapsed().as_micros() as u64,
                            "signed and sent buy_exact_sol_in transaction"
                        );
                    }
                    Err(e) => warn!("failed to send transaction: {e}"),
                }
                #[cfg(feature = "dry-run")]
                {
                    info!(
                        slot = trigger.slot,
                        mint = %tx_mint,
                        creator = %creator,
                        creator_vault = %addresses.creator_vault,
                        blockhash = %blockhash,
                        local_sig = %local_sig,
                        "dry-run: transaction signed (not sent)"
                    );
                    dispatch_placeholder(&http, &args.sender_url).await;
                }
            }
            Err(e) => warn!("failed to build/sign tx: {e}"),
        }
    }
}

#[derive(Clone, Copy)]
struct PumpTradeAddresses {
    global: Pubkey,
    bonding_curve: Pubkey,
    associated_bonding_curve: Pubkey,
    fee_recipient: Pubkey,
    creator_vault: Pubkey,
    event_authority: Pubkey,
    global_volume_accumulator: Pubkey,
}

fn parse_pubkey(value: &str) -> Pubkey {
    Pubkey::from_str(value).expect("static pubkey constant must be valid")
}

fn derive_ata(owner: &Pubkey, mint: &Pubkey, token_program: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[owner.as_ref(), token_program.as_ref(), mint.as_ref()],
        &parse_pubkey(ASSOCIATED_TOKEN_PROGRAM_ID),
    )
    .0
}

fn derive_pump_trade_addresses(
    pump_program: Pubkey,
    mint: Pubkey,
    creator: Option<Pubkey>,
) -> PumpTradeAddresses {
    let token_program = parse_pubkey(TOKEN_PROGRAM_ID);
    let bonding_curve =
        Pubkey::find_program_address(&[b"bonding-curve", mint.as_ref()], &pump_program).0;
    let associated_bonding_curve = derive_ata(&bonding_curve, &mint, &token_program);
    let global = Pubkey::find_program_address(&[b"global"], &pump_program).0;
    let global_volume_accumulator =
        Pubkey::find_program_address(&[b"global_volume_accumulator"], &pump_program).0;
    let creator_vault = creator
        .map(|c| Pubkey::find_program_address(&[b"creator-vault", c.as_ref()], &pump_program).0)
        .unwrap_or_default();

    PumpTradeAddresses {
        global,
        bonding_curve,
        associated_bonding_curve,
        fee_recipient: parse_pubkey(PUMP_FEE_RECIPIENT),
        creator_vault,
        event_authority: parse_pubkey(PUMP_EVENT_AUTHORITY),
        global_volume_accumulator,
    }
}

fn build_create_ata_idempotent_ix(payer: Pubkey, owner: Pubkey, mint: Pubkey) -> Instruction {
    let token_program = parse_pubkey(TOKEN_PROGRAM_ID);
    let ata = derive_ata(&owner, &mint, &token_program);
    Instruction {
        program_id: parse_pubkey(ASSOCIATED_TOKEN_PROGRAM_ID),
        accounts: vec![
            AccountMeta::new(payer, true),
            AccountMeta::new(ata, false),
            AccountMeta::new_readonly(owner, false),
            AccountMeta::new_readonly(mint, false),
            AccountMeta::new_readonly(system_program::id(), false),
            AccountMeta::new_readonly(token_program, false),
        ],
        data: vec![1],
    }
}

fn build_buy_exact_sol_in_ix(
    pump_program: Pubkey,
    mint: Pubkey,
    buyer: Pubkey,
    addresses: PumpTradeAddresses,
    sol_amount: u64,
    min_token_amount: u64,
) -> Instruction {
    let token_program = parse_pubkey(TOKEN_PROGRAM_ID);
    let user_token_ata = derive_ata(&buyer, &mint, &token_program);
    let user_volume_accumulator =
        Pubkey::find_program_address(&[b"user_volume_accumulator", buyer.as_ref()], &pump_program)
            .0;

    let mut data = vec![0u8; 25];
    data[0..8].copy_from_slice(&BUY_EXACT_SOL_IN_DISCRIMINATOR);
    data[8..16].copy_from_slice(&sol_amount.to_le_bytes());
    data[16..24].copy_from_slice(&min_token_amount.to_le_bytes());
    data[24] = 1;

    Instruction {
        program_id: pump_program,
        accounts: vec![
            AccountMeta::new_readonly(addresses.global, false),
            AccountMeta::new(addresses.fee_recipient, false),
            AccountMeta::new_readonly(mint, false),
            AccountMeta::new(addresses.bonding_curve, false),
            AccountMeta::new(addresses.associated_bonding_curve, false),
            AccountMeta::new(user_token_ata, false),
            AccountMeta::new(buyer, true),
            AccountMeta::new_readonly(system_program::id(), false),
            AccountMeta::new_readonly(token_program, false),
            AccountMeta::new(addresses.creator_vault, false),
            AccountMeta::new_readonly(addresses.event_authority, false),
            AccountMeta::new_readonly(pump_program, false),
            AccountMeta::new(addresses.global_volume_accumulator, false),
            AccountMeta::new(user_volume_accumulator, false),
            AccountMeta::new_readonly(parse_pubkey(PUMP_FEE_CONFIG_ACCOUNT), false),
            AccountMeta::new_readonly(parse_pubkey(PUMP_FEE_PROGRAM), false),
        ],
        data,
    }
}

fn build_signed_trade_tx(
    recent_blockhash: Hash,
    signer: &Keypair,
    pump_program: Pubkey,
    mint: Pubkey,
    addresses: PumpTradeAddresses,
    cu_limit: u32,
    cu_price_micro_lamports: u64,
    sol_amount: u64,
    min_token_amount: u64,
    tip_lamports: u64,
) -> Result<Transaction> {
    let payer = signer.pubkey();
    let mut ixs = vec![
        ComputeBudgetInstruction::set_compute_unit_limit(cu_limit),
        ComputeBudgetInstruction::set_compute_unit_price(cu_price_micro_lamports),
        build_create_ata_idempotent_ix(payer, payer, mint),
        build_buy_exact_sol_in_ix(
            pump_program,
            mint,
            payer,
            addresses,
            sol_amount,
            min_token_amount,
        ),
    ];
    #[cfg(not(feature = "dry-run"))]
    if tip_lamports > 0 {
        ixs.push(system_instruction::transfer(
            &payer,
            &parse_pubkey(FALCON_TIP_ACCOUNT),
            tip_lamports,
        ));
    }
    let message = Message::new(&ixs, Some(&payer));
    let mut tx = Transaction::new_unsigned(message);
    tx.try_sign(&[signer], recent_blockhash)
        .context("failed to sign transaction")?;
    Ok(tx)
}

async fn run_blockhash_loop(
    rpc_url: String,
    blockhash_tx: watch::Sender<Option<Hash>>,
) -> Result<()> {
    let rpc = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::finalized());
    let mut interval = tokio::time::interval(Duration::from_millis(500));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        match rpc.get_latest_blockhash().await {
            Ok(blockhash) => {
                let _ = blockhash_tx.send(Some(blockhash));
            }
            Err(e) => warn!("failed to fetch finalized blockhash: {e}"),
        }
    }
}

fn tip_sol_to_lamports(tip_sol: f64) -> Result<u64> {
    if !tip_sol.is_finite() || tip_sol < 0.0 {
        return Err(anyhow!("--tip-sol must be a non-negative finite number"));
    }
    let lamports = tip_sol * LAMPORTS_PER_SOL as f64;
    if lamports > u64::MAX as f64 {
        return Err(anyhow!("--tip-sol is too large"));
    }
    Ok(lamports.round() as u64)
}

#[cfg(feature = "dry-run")]
async fn dispatch_placeholder(_client: &Client, _sender_url: &str) {
    info!("dry-run enabled: trigger matched; tx-building/send is not implemented yet");
}

#[cfg(not(feature = "dry-run"))]
async fn send_signed_transaction(
    client: &Client,
    sender_url: &str,
    tx: &Transaction,
) -> Result<Signature> {
    let tx_bytes = bincode::serialize(tx).context("failed to serialize transaction")?;
    let tx_b64 = BASE64_STANDARD.encode(tx_bytes);
    let request_body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "sendTransaction",
        "params": [
            tx_b64,
            {
                "encoding": "base64",
                "skipPreflight": true,
                "maxRetries": 0,
                "preflightCommitment": "processed"
            }
        ]
    });

    let response = client
        .post(sender_url)
        .json(&request_body)
        .send()
        .await
        .context("sender sendTransaction request failed")?;
    let status = response.status();
    let payload: serde_json::Value = response
        .json()
        .await
        .context("failed to parse sender response json")?;
    if !status.is_success() {
        return Err(anyhow!("sender http status {status}: {payload}"));
    }
    if let Some(err) = payload.get("error") {
        return Err(anyhow!("sender returned error: {err}"));
    }
    let sig_str = payload
        .get("result")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("sender response missing result signature: {payload}"))?;
    Signature::from_str(sig_str).context("sender returned invalid signature")
}

#[cfg(not(feature = "dry-run"))]
async fn run_sender_keepalive(http: Client, sender_url: String, ping_interval: Duration) {
    let mut interval = tokio::time::interval(ping_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        if let Err(e) = warm_sender_once(&http, &sender_url).await {
            warn!("sender keepalive failed: {e}");
        }
    }
}

#[cfg(not(feature = "dry-run"))]
async fn warm_sender_once(client: &Client, sender_url: &str) -> Result<()> {
    let request_body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getHealth",
        "params": []
    });

    client
        .post(sender_url)
        .json(&request_body)
        .send()
        .await
        .context("sender keepalive request failed")?;

    Ok(())
}
