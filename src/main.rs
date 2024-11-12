use std::sync::atomic::{AtomicU64, Ordering};

use rocksdb::{Options, SliceTransform, DB};
use serde::{Deserialize, Serialize};

static SUFFIX_PROVIDER: AtomicU64 = AtomicU64::new(0);
fn get_next_number() -> u64 {
    // Use fetch_add to atomically increment the counter and return the previous value
    SUFFIX_PROVIDER.fetch_add(1, Ordering::SeqCst)
}

#[derive(Debug)]
struct CoinIndexDef {
    user: String,
    asset: String,
    amount: u64,
    main_db_key: Vec<u8>,
}

impl CoinIndexDef {
    fn new(user: String, asset: String, amount: u64, main_db_key: Vec<u8>) -> Self {
        Self {
            user,
            asset,
            amount,
            main_db_key,
        }
    }
}

impl core::fmt::Display for CoinIndexDef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Coin Index {{ user: {}, asset: {}, amount: {}, main_db_key: {:?} }}",
            self.user,
            self.asset,
            self.amount,
            hex::encode(&self.main_db_key)
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CoinDef {
    amount: u64,
    metadata: String,
}

struct CoinsManager {
    main_db: DB,
    index_db: DB,
}

impl CoinsManager {
    fn new() -> Self {
        let mut main_options = Options::default();
        main_options.create_if_missing(true);
        let main_db = DB::open(&main_options, "main_db").unwrap();

        let mut indexing_options = Options::default();
        indexing_options.create_if_missing(true);
        indexing_options.set_prefix_extractor(SliceTransform::create_fixed_prefix(32)); // 16 for user + 16 for asset
        let index_db = DB::open(&indexing_options, "index_db").unwrap();
        Self { main_db, index_db }
    }

    fn add_coin(
        &self,
        amount: u64,
        user: impl ToString + core::fmt::Display + Clone,
        asset: impl ToString + core::fmt::Display + Clone,
    ) {
        let user_as_hex = normalize_string(user.clone());
        let asset_as_hex = normalize_string(asset.clone());
        let nonce = normalize_string(rand::random::<u64>().to_string());

        println!("Adding: user={user}, asset={asset}, amount={amount}");

        // Main DB
        let main_key = format!("{user_as_hex}{asset_as_hex}{nonce}");
        let metadata = CoinDef {
            amount,
            metadata: format!("Some additional info about the coin from main_db. For example, a random number: {}", rand::random::<u64>()),
        };
        let serialized_metadata = postcard::to_allocvec(&metadata).unwrap();
        self.main_db
            .put(main_key.clone(), serialized_metadata)
            .unwrap();

        // Indexation DB
        let serialized_amount = postcard::to_allocvec(&amount.to_be_bytes()).unwrap();
        let serialized_amount_hex = hex::encode(serialized_amount);
        let suffix = get_next_number() as u64;
        let serialized_suffix = postcard::to_allocvec(&suffix.to_be_bytes()).unwrap();
        let serialized_suffix_hex = hex::encode(serialized_suffix);
        let key =
            format!("{user_as_hex}{asset_as_hex}{serialized_amount_hex}{serialized_suffix_hex}");
        println!("Indexing: key={key} length={}", key.len());
        self.index_db.put(key, main_key).unwrap();
    }

    fn get_coins(
        &self,
        user: impl ToString + core::fmt::Display + Clone,
        asset: impl ToString + core::fmt::Display + Clone,
    ) -> Vec<CoinIndexDef> {
        let user_as_hex = normalize_string(user.clone());
        let asset_as_hex = normalize_string(asset.clone());

        let prefix = format!("{user_as_hex}{asset_as_hex}");
        println!("Reading: user={user}, asset={asset} --> prefix={prefix}");
        self.index_db
            .prefix_iterator(prefix)
            .map(|entry| {
                let (full_key, main_db_key) = entry.unwrap();

                let user = &full_key[..16];
                let asset = &full_key[16..32];
                let amount = &full_key[32..48];
                let _suffix = &full_key[48..];

                let user_ascii: String = hex::decode(user)
                    .unwrap()
                    .iter()
                    .map(|b| *b as char)
                    .collect();

                let asset_ascii: String = hex::decode(asset)
                    .unwrap()
                    .iter()
                    .map(|b| *b as char)
                    .collect();

                let amount = u64::from_be_bytes(hex::decode(amount).unwrap().try_into().unwrap());

                CoinIndexDef::new(user_ascii, asset_ascii, amount, main_db_key.to_vec())
            })
            .collect()
    }
}

// Makes the string exactly 8 characters long by truncating or adding trailing dots.
fn normalize_string(s: impl ToString + Clone) -> String {
    hex::encode(make_string_exactly_8_chars_long(s.clone()))
}

fn make_string_exactly_8_chars_long(s: impl ToString) -> String {
    let mut s = s.to_string();
    if s.len() == 8 {
        return s;
    }
    if s.len() > 8 {
        s.truncate(8);
        return s;
    }
    while s.len() < 8 {
        s.push('.');
    }
    s
}

impl Drop for CoinsManager {
    fn drop(&mut self) {
        println!("Dropping CoinsManager");
        let _ = DB::destroy(&Options::default(), "main_db");
        let _ = DB::destroy(&Options::default(), "index_db");

        let _ = std::fs::remove_dir_all("main_db");
        let _ = std::fs::remove_dir_all("index_db");
    }
}

fn main() {
    let cm = CoinsManager::new();

    // Two distinct coins
    cm.add_coin(1, "Alice", "BTC");
    cm.add_coin(2, "Alice", "ETH");

    // Two distinct coins
    cm.add_coin(3, "Bob", "BTC");
    cm.add_coin(4, "Bob", "LCK");

    // Single coin
    cm.add_coin(5, "Charlie", "BTC");

    // Two identical coins
    cm.add_coin(6, "Dave", "ETH");
    cm.add_coin(6, "Dave", "ETH");

    // Same value, different asset ID
    cm.add_coin(7, "Eve", "BTC");
    cm.add_coin(7, "Eve", "ETH");

    // Alice also has 1 BTC, this differs only by user
    cm.add_coin(1, "Frank", "BTC");

    // Many coins and different assets
    cm.add_coin(2, "Grace", "BTC");
    cm.add_coin(3, "Grace", "BTC");
    cm.add_coin(1, "Grace", "ETH");
    cm.add_coin(3, "Grace", "ETH");
    cm.add_coin(1, "Grace", "BTC");
    cm.add_coin(2, "Grace", "ETH");

    // Do some retrievals
    let coins = cm.get_coins("Dave", "ETH");
    dump_coins(&cm.main_db, &coins);

    let coins = cm.get_coins("Grace", "ETH");
    dump_coins(&cm.main_db, &coins);

    let coins = cm.get_coins("Grace", "BTC");
    dump_coins(&cm.main_db, &coins);

    let coins = cm.get_coins("Charlie", "LCK");
    dump_coins(&cm.main_db, &coins);

    let coins = cm.get_coins("Bob", "LCK");
    dump_coins(&cm.main_db, &coins);
}

fn dump_coins(main_db: &DB, coins: &[CoinIndexDef]) {
    println!("\tCoins Index:");
    for coin in coins {
        let main_db_key = coin.main_db_key.clone();
        let serialized_metadata = main_db.get(&main_db_key).unwrap().unwrap();
        let deserialized_metadata: CoinDef = postcard::from_bytes(&serialized_metadata).unwrap();

        println!("\t\t{}", coin);
        println!("\t\t\tMetadata: {:?}", deserialized_metadata);
    }
}
