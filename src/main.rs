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

impl PartialEq for CoinIndexDef {
    fn eq(&self, other: &Self) -> bool {
        self.user == other.user && self.asset == other.asset && self.amount == other.amount
    }
}

impl From<&(&str, &str, u64)> for CoinIndexDef {
    fn from((user, asset, amount): &(&str, &str, u64)) -> Self {
        Self::new(
            make_string_exactly_8_chars_long(user.to_string()),
            make_string_exactly_8_chars_long(asset.to_string()),
            *amount,
            vec![],
        )
    }
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
        user: impl ToString + core::fmt::Display + Clone,
        asset: impl ToString + core::fmt::Display + Clone,
        amount: u64,
    ) {
        let user_as_hex = normalize_string(user.clone());
        let asset_as_hex = normalize_string(asset.clone());
        let nonce = normalize_string(rand::random::<u64>().to_string());

        // println!("Adding: user={user}, asset={asset}, amount={amount}");

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
        //println!("Indexing: key={key} length={}", key.len());
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
        //println!("Reading: user={user}, asset={asset} --> prefix={prefix}");
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
        //println!("Dropping CoinsManager");
        let _ = DB::destroy(&Options::default(), "main_db");
        let _ = DB::destroy(&Options::default(), "index_db");

        let _ = std::fs::remove_dir_all("main_db");
        let _ = std::fs::remove_dir_all("index_db");
    }
}

fn main() {
    /*
    let cm = CoinsManager::new();

    // Two distinct coins
    cm.add_coin("Alice", "BTC", 1);
    cm.add_coin("Alice", "ETH", 2);

    // Two distinct coins
    cm.add_coin("Bob", "BTC", 3);
    cm.add_coin("Bob", "LCK", 4);

    // Single coin
    cm.add_coin("Charlie", "BTC", 5);

    // Two identical coins
    cm.add_coin("Dave", "ETH", 6);
    cm.add_coin("Dave", "ETH", 7);

    // Same value, different asset ID
    cm.add_coin("Eve", "BTC", 8);
    cm.add_coin("Eve", "ETH", 8);

    // Alice also has 1 BTC, this differs only by user
    cm.add_coin("Frank", "BTC", 1);

    // Many coins and different assets
    cm.add_coin("Grace", "BTC", 2);
    cm.add_coin("Grace", "BTC", 3);
    cm.add_coin("Grace", "ETH", 1);
    cm.add_coin("Grace", "ETH", 3);
    cm.add_coin("Grace", "BTC", 1);
    cm.add_coin("Grace", "ETH", 2);

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
    */
    println!("Use cargo test");
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use rand::seq::SliceRandom;
    use rocksdb::DB;
    use test_case::test_case;

    use crate::{normalize_string, CoinDef, CoinIndexDef, CoinsManager};

    #[derive(Debug)]
    struct TestCase {
        coins: &'static [(&'static str, &'static str, u64)],
        owner: &'static str,
        asset: &'static str,
        expected_coins: &'static [(&'static str, &'static str, u64)],
    }

    #[rustfmt::skip]
    const COIN_DATABASE: &'static [(&'static str, &'static str, u64)] =
        &[
            ("Alice", "BTC", 1),
            ("Alice", "ETH", 2),

            ("Bob", "BTC", 3),
            ("Bob", "LCK", 4),

            ("Charlie", "BTC", 5),

            // Two identical coins
            ("Dave", "LCK", 10),
            ("Dave", "LCK", 10),

            ("Eve", "BTC", 8),
            ("Eve", "ETH", 8),

            ("Frank", "BTC", 1),
            
            ("Grace", "BTC", 2),
            ("Grace", "BTC", 3),
            ("Grace", "ETH", 1),
            ("Grace", "ETH", 3),
            ("Grace", "BTC", 1),
            ("Grace", "ETH", 2),
        ];

    const SINGLE_COIN_1: TestCase = TestCase {
        coins: COIN_DATABASE,
        owner: "Alice",
        asset: "BTC",
        expected_coins: &[("Alice", "BTC", 1)],
    };

    const SINGLE_COIN_2: TestCase = TestCase {
        coins: COIN_DATABASE,
        owner: "Alice",
        asset: "ETH",
        expected_coins: &[("Alice", "ETH", 2)],
    };

    const MULTIPLE_COINS: TestCase = TestCase {
        coins: COIN_DATABASE,
        owner: "Grace",
        asset: "BTC",
        expected_coins: &[
            ("Grace", "BTC", 1),
            ("Grace", "BTC", 2),
            ("Grace", "BTC", 3),
        ],
    };

    const NO_COINS_MISSING_ASSET: TestCase = TestCase {
        coins: COIN_DATABASE,
        owner: "Charlie",
        asset: "LCK",
        expected_coins: &[],
    };

    const NO_COINS_MISSING_OWNER: TestCase = TestCase {
        coins: COIN_DATABASE,
        owner: "NON_EXISTENT_OWNER",
        asset: "LCK",
        expected_coins: &[],
    };

    const COINS_DO_NOT_CONFLICT: TestCase = TestCase {
        coins: COIN_DATABASE,
        owner: "Dave",
        asset: "LCK",
        expected_coins: &[("Dave", "LCK", 10), ("Dave", "LCK", 10)],
    };

    #[test_case(SINGLE_COIN_1; "Single coin retrieval 1")]
    #[test_case(SINGLE_COIN_2; "Single coin retrieval 2")]
    #[test_case(MULTIPLE_COINS; "Multiple coins retrieval")]
    #[test_case(NO_COINS_MISSING_ASSET; "No coins due to missing asset")]
    #[test_case(NO_COINS_MISSING_OWNER; "No coins due to missing owner")]
    #[test_case(COINS_DO_NOT_CONFLICT; "Coins do not conflict")]
    fn coin_retrieval(
        TestCase {
            coins,
            owner,
            asset,
            expected_coins,
        }: TestCase,
    ) {
        let cm = make_coin_manager(coins);
        let actual_coins = cm.get_coins(owner, asset);
        assert_coins(&expected_coins, &actual_coins, &cm.main_db);
    }

    fn make_coin_manager(coins: &'static [(&'static str, &'static str, u64)]) -> CoinsManager {
        let cm = CoinsManager::new();

        // Shuffle coins before feeding them to the coin manager.
        let mut shuffled_coins = coins.to_vec();
        shuffled_coins.shuffle(&mut rand::thread_rng());

        for (user, asset, amount) in shuffled_coins {
            cm.add_coin(user, asset, amount);
        }
        cm
    }

    fn assert_coins(
        expected: &[(&'static str, &'static str, u64)],
        actual: &[CoinIndexDef],
        main_db: &DB,
    ) {
        // Correct coins are returned.
        let expected: Vec<CoinIndexDef> = expected.iter().map(Into::into).collect::<Vec<_>>();
        assert_eq!(expected, actual);

        // Coins are sorted.
        let amounts = actual.iter().map(|coin| coin.amount).collect::<Vec<_>>();
        assert!(amounts.is_sorted());

        // Check link to main DB
        let mut unique_metadata = BTreeSet::new();
        for coin in actual {
            let serialized_metadata = main_db.get(&coin.main_db_key).unwrap().unwrap();
            let deserialized_metadata: CoinDef =
                postcard::from_bytes(&serialized_metadata).unwrap();
            assert!(!deserialized_metadata.metadata.is_empty());
            unique_metadata.insert(deserialized_metadata.metadata.clone());
        }

        // Make sure that metadata are unique, ie. link to main DB is correct even for identical coins.
        assert_eq!(unique_metadata.len(), actual.len());
    }
}
