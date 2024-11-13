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
    utxo_id: String,
    main_db_key: Vec<u8>,
}

impl PartialEq for CoinIndexDef {
    fn eq(&self, other: &Self) -> bool {
        self.user == other.user
            && self.asset == other.asset
            && self.amount == other.amount
            && self.utxo_id == other.utxo_id
    }
}

impl CoinIndexDef {
    fn new(
        user: String,
        asset: String,
        amount: u64,
        main_db_key: Vec<u8>,
        utxo_id: String,
    ) -> Self {
        Self {
            user,
            asset,
            amount,
            main_db_key,
            utxo_id,
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
    utxo_id: String,
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

    fn add_coin<S: ToString + core::fmt::Display + Clone>(
        &self,
        utxo_id: S,
        user: S,
        asset: S,
        amount: u64,
    ) {
        let user_as_hex = normalize_string(user.clone());
        let asset_as_hex = normalize_string(asset.clone());
        let nonce = normalize_string(rand::random::<u64>().to_string());

        // println!("Adding: user={user}, asset={asset}, amount={amount}");

        // IMPORTANT: Both of these writes must happen in the same transaction!

        // Main DB
        let main_key = format!("{user_as_hex}{asset_as_hex}{nonce}");
        let metadata = CoinDef {
            amount,
            metadata: format!(
                "Metadata: utxo_id={}, nonce={}",
                utxo_id,
                rand::random::<u64>()
            ),
            utxo_id: utxo_id.to_string(),
        };
        let serialized_metadata = postcard::to_allocvec(&metadata).unwrap();
        self.main_db
            .put(main_key.clone(), serialized_metadata)
            .unwrap();

        // Indexation DB
        let serialized_amount = postcard::to_allocvec(&amount.to_be_bytes()).unwrap();
        let serialized_amount_hex = hex::encode(serialized_amount);
        //let suffix = get_next_number() as u64;
        let suffix = utxo_id.to_string();
        let key = format!("{user_as_hex}{asset_as_hex}{serialized_amount_hex}{suffix}");
        //println!("Indexing: key={key} length={}", key.len());
        self.index_db.put(key, main_key).unwrap();
    }

    fn iter<S>(&self, user: S, asset: S) -> impl Iterator<Item = CoinIndexDef> + '_
    where
        S: ToString + core::fmt::Display + Clone,
    {
        let user_as_hex = normalize_string(user.clone());
        let asset_as_hex = normalize_string(asset.clone());

        let prefix = format!("{user_as_hex}{asset_as_hex}");
        //println!("Reading: user={user}, asset={asset} --> prefix={prefix}");
        self.index_db.prefix_iterator(prefix).map(|entry| {
            let (full_key, main_db_key) = entry.unwrap();

            let user = &full_key[..16];
            let asset = &full_key[16..32];
            let amount = &full_key[32..48];
            let utxo_id = &full_key[48..];

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

            let utxo_id_ascii: String = utxo_id.iter().map(|b| *b as char).collect();

            let amount = u64::from_be_bytes(hex::decode(amount).unwrap().try_into().unwrap());

            CoinIndexDef::new(
                user_ascii,
                asset_ascii,
                amount,
                main_db_key.to_vec(),
                utxo_id_ascii,
            )
        })
    }

    fn get_coins<S>(&self, user: S, asset: S, excluded_utxo_ids: &[String]) -> Vec<CoinIndexDef>
    where
        S: ToString + core::fmt::Display + Clone,
    {
        self.iter(user, asset)
            .filter(|coin| !excluded_utxo_ids.contains(&coin.utxo_id))
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
    let x: u64 = 54321;
    println!("{}, {}", x, hex::encode(x.to_be_bytes()));

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

    use crate::{make_string_exactly_8_chars_long, CoinDef, CoinIndexDef, CoinsManager};

    type UtxoIdDef = str;
    type OwnerDef = str;
    type AssetDef = str;
    type AmountDef = u64;

    type Coin = (
        &'static UtxoIdDef,
        &'static OwnerDef,
        &'static AssetDef,
        u64,
    );
    type CoinDatabase = &'static [Coin];

    impl From<&(&UtxoIdDef, &OwnerDef, &AssetDef, AmountDef)> for CoinIndexDef {
        fn from(
            (utxo_id, user, asset, amount): &(&UtxoIdDef, &OwnerDef, &AssetDef, AmountDef),
        ) -> Self {
            Self::new(
                make_string_exactly_8_chars_long(user.to_string()),
                make_string_exactly_8_chars_long(asset.to_string()),
                *amount,
                vec![],
                utxo_id.to_string(),
            )
        }
    }

    #[derive(Debug)]
    struct TestCase {
        coins: CoinDatabase,
        owner: &'static str,
        asset: &'static str,
        expected_coins: &'static [Coin],
        excluded_utxos: &'static str,
    }

    #[rustfmt::skip]
    const COIN_DATABASE: CoinDatabase =
        &[
            ("UTXO00", "Alice", "BTC", 1),
            ("UTXO01", "Alice", "ETH", 2),
 
            ("UTXO02", "Bob", "BTC", 3),
            ("UTXO03", "Bob", "LCK", 4),
 
            ("UTXO04", "Charlie", "BTC", 5),
 
            ("UTXO05", "Dave", "LCK", 10),
            ("UTXO06", "Dave", "LCK", 10),
            ("UTXO07", "Dave", "BTC", 11),
            ("UTXO08", "Dave", "BTC", 11),
 
            ("UTXO09", "Eve", "BTC", 8),
            ("UTXO10", "Eve", "ETH", 8),
 
            ("UTXO11", "Frank", "BTC", 1),
             
            ("UTXO12", "Grace", "BTC", 2),
            ("UTXO13", "Grace", "BTC", 3),
            ("UTXO14", "Grace", "ETH", 1),
            ("UTXO15", "Grace", "ETH", 3),
            ("UTXO16", "Grace", "BTC", 1),
            ("UTXO17", "Grace", "ETH", 2),
        ];

    const SINGLE_COIN_1: TestCase = TestCase {
        coins: COIN_DATABASE,
        owner: "Alice",
        asset: "BTC",
        excluded_utxos: "",
        expected_coins: &[("UTXO00", "Alice", "BTC", 1)],
    };

    const SINGLE_COIN_2: TestCase = TestCase {
        coins: COIN_DATABASE,
        owner: "Alice",
        asset: "ETH",
        excluded_utxos: "",
        expected_coins: &[("UTXO01", "Alice", "ETH", 2)],
    };

    const MULTIPLE_COINS: TestCase = TestCase {
        coins: COIN_DATABASE,
        owner: "Grace",
        asset: "BTC",
        excluded_utxos: "",
        expected_coins: &[
            ("UTXO16", "Grace", "BTC", 1),
            ("UTXO12", "Grace", "BTC", 2),
            ("UTXO13", "Grace", "BTC", 3),
        ],
    };

    const NO_COINS_MISSING_ASSET: TestCase = TestCase {
        coins: COIN_DATABASE,
        owner: "Charlie",
        asset: "LCK",
        excluded_utxos: "",
        expected_coins: &[],
    };

    const NO_COINS_MISSING_OWNER: TestCase = TestCase {
        coins: COIN_DATABASE,
        owner: "NON_EXISTENT_OWNER",
        asset: "LCK",
        excluded_utxos: "",
        expected_coins: &[],
    };

    const COINS_DO_NOT_CONFLICT_1: TestCase = TestCase {
        coins: COIN_DATABASE,
        owner: "Dave",
        asset: "LCK",
        excluded_utxos: "",
        expected_coins: &[("UTXO05", "Dave", "LCK", 10), ("UTXO06", "Dave", "LCK", 10)],
    };

    const COINS_DO_NOT_CONFLICT_2: TestCase = TestCase {
        coins: COIN_DATABASE,
        owner: "Dave",
        asset: "BTC",
        excluded_utxos: "",
        expected_coins: &[("UTXO07", "Dave", "BTC", 11), ("UTXO08", "Dave", "BTC", 11)],
    };

    #[test_case(SINGLE_COIN_1; "Single coin retrieval 1")]
    #[test_case(SINGLE_COIN_2; "Single coin retrieval 2")]
    #[test_case(MULTIPLE_COINS; "Multiple coins retrieval")]
    #[test_case(NO_COINS_MISSING_ASSET; "No coins due to missing asset")]
    #[test_case(NO_COINS_MISSING_OWNER; "No coins due to missing owner")]
    #[test_case(COINS_DO_NOT_CONFLICT_1; "Coins do not conflict 1")]
    #[test_case(COINS_DO_NOT_CONFLICT_2; "Coins do not conflict 2")]
    fn coin_retrieval(
        TestCase {
            coins,
            owner,
            asset,
            expected_coins,
            ..
        }: TestCase,
    ) {
        const NO_EXCLUDED_UTXO_IDS: &[String] = &[];

        let cm = make_coin_manager(coins);
        let actual_coins: Vec<_> = cm.get_coins(owner, asset, NO_EXCLUDED_UTXO_IDS);
        assert_coins(&expected_coins, &actual_coins, &cm.main_db);
    }

    const EXCLUDE_SINGLE_UTXO: TestCase = TestCase {
        coins: COIN_DATABASE,
        owner: "Grace",
        asset: "BTC",
        excluded_utxos: "UTXO12",
        expected_coins: &[("UTXO16", "Grace", "BTC", 1), ("UTXO13", "Grace", "BTC", 3)],
    };

    const EXCLUDE_ALL_UTXO: TestCase = TestCase {
        coins: COIN_DATABASE,
        owner: "Grace",
        asset: "BTC",
        excluded_utxos: "UTXO12;UTXO16;UTXO13",
        expected_coins: &[],
    };

    const EXCLUDE_NON_EXISTENT_UTXO: TestCase = TestCase {
        coins: COIN_DATABASE,
        owner: "Dave",
        asset: "LCK",
        excluded_utxos: "NON_EXISTENT_UTXO",
        expected_coins: &[("UTXO05", "Dave", "LCK", 10), ("UTXO06", "Dave", "LCK", 10)],
    };

    const EXCLUDE_WITH_IDENTICAL_COINS: TestCase = TestCase {
        coins: COIN_DATABASE,
        owner: "Dave",
        asset: "BTC",
        excluded_utxos: "UTXO07",
        expected_coins: &[("UTXO08", "Dave", "BTC", 11)],
    };

    #[test_case(EXCLUDE_SINGLE_UTXO; "Exclude single UTXO")]
    #[test_case(EXCLUDE_ALL_UTXO; "Exclude all UTXO")]
    #[test_case(EXCLUDE_NON_EXISTENT_UTXO; "Exclude non existent UTXO has no effect")]
    #[test_case(EXCLUDE_WITH_IDENTICAL_COINS; "Exclude correct UTXO with identical coins")]
    fn exclude_coin(
        TestCase {
            coins,
            owner,
            asset,
            excluded_utxos,
            expected_coins,
        }: TestCase,
    ) {
        let excluded_utxo_ids = excluded_utxos
            .split(';')
            .map(Into::into)
            .collect::<Vec<_>>();

        let cm = make_coin_manager(coins);
        let actual_coins: Vec<_> = cm.get_coins(owner, asset, &excluded_utxo_ids);

        assert_coins(&expected_coins, &actual_coins, &cm.main_db);
    }

    fn make_coin_manager(coins: CoinDatabase) -> CoinsManager {
        let cm = CoinsManager::new();

        // Shuffle coins before feeding them to the coin manager.
        let mut shuffled_coins = coins.to_vec();
        shuffled_coins.shuffle(&mut rand::thread_rng());

        for (utxo_id, user, asset, amount) in shuffled_coins {
            cm.add_coin(utxo_id, user, asset, amount);
        }
        cm
    }

    fn assert_coins(expected: &[Coin], actual: &[CoinIndexDef], main_db: &DB) {
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

        // Make sure that metadata are unique, ie. link to main DB is correct even for "identical" coins.
        assert_eq!(unique_metadata.len(), actual.len());
    }
}
