use std::{
    collections::BTreeMap,
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
};

use rocksdb::{Options, SliceTransform, DB};
use serde::{Deserialize, Serialize};

static SUFFIX_PROVIDER: AtomicU64 = AtomicU64::new(0);
fn get_next_number() -> u64 {
    // Use fetch_add to atomically increment the counter and return the previous value
    SUFFIX_PROVIDER.fetch_add(1, Ordering::SeqCst)
}

#[derive(Debug, Clone)]
pub struct CoinIndexDef {
    user: String,
    asset: String,
    amount: u64,
    utxo_id: String,
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
    fn new(user: String, asset: String, amount: u64, utxo_id: String) -> Self {
        Self {
            user,
            asset,
            amount,
            utxo_id,
        }
    }
}

impl core::fmt::Display for CoinIndexDef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Coin Index {{ user: {}, asset: {}, amount: {} }}",
            self.user, self.asset, self.amount,
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

#[derive(Debug)]
struct Query {
    asset: String,
    amount: u64,
    max: Option<u32>,
}

impl FromStr for Query {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split(';');
        let asset_id = parts.next().ok_or(())?.to_string();
        let amount = parts.next().ok_or(())?.parse().map_err(|_| ())?;
        let max = parts.next().ok_or(())?.parse().ok();
        Ok(Self {
            asset: asset_id,
            amount,
            max,
        })
    }
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

        // println!("Adding: user={user}, asset={asset}, amount={amount}");

        // IMPORTANT: Both of these writes must happen in the same transaction!

        // Main DB
        let main_key = format!("{utxo_id}");
        let metadata = CoinDef {
            amount,
            metadata: format!(
                "Metadata: utxo_id={}, nonce={}",
                utxo_id,
                rand::random::<u64>()
            ),
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
        self.index_db.put(key, &[]).unwrap();
    }

    fn iter<S, T>(&self, user: S, asset: T) -> impl Iterator<Item = CoinIndexDef> + '_
    where
        S: ToString + core::fmt::Display + Clone,
        T: ToString + core::fmt::Display + Clone,
    {
        let user_as_hex = normalize_string(user.clone());
        let asset_as_hex = normalize_string(asset.clone());

        let prefix = format!("{user_as_hex}{asset_as_hex}");
        //println!("Reading: user={user}, asset={asset} --> prefix={prefix}");
        self.index_db.prefix_iterator(prefix).map(|entry| {
            let (full_key, _) = entry.unwrap();

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

            CoinIndexDef::new(user_ascii, asset_ascii, amount, utxo_id_ascii)
        })
    }

    fn coins<S, T>(&self, user: S, asset: T, excluded_utxo_ids: &[String]) -> Vec<CoinIndexDef>
    where
        S: ToString + core::fmt::Display + Clone,
        T: ToString + core::fmt::Display + Clone,
    {
        self.iter(user, asset)
            .filter(|coin| !excluded_utxo_ids.contains(&coin.utxo_id))
            .collect()
    }

    fn coins_to_spend<S>(
        &self,
        user: S,
        q: &[Query],
        excluded_utxo_ids: &[String],
    ) -> Vec<Vec<CoinIndexDef>>
    where
        S: ToString + core::fmt::Display + Clone,
    {
        let eligible_coins: Vec<_> = q
            .iter()
            .map(|q| {
                let eligible_coins_per_asset = self.coins(&user, &q.asset, excluded_utxo_ids);
                eligible_coins_per_asset
            })
            .collect();

        dbg!(&eligible_coins);

        q.iter()
            .zip(eligible_coins.iter())
            .map(|(Query { asset, amount, max }, eligible_coins)| {
                // Take largest coins until:
                // - the amount is reached
                // - the max number of coins is reached

                let mut sum = 0;
                let selected_coins: Vec<_> = eligible_coins
                    .iter()
                    .rev()
                    .take_while(|coin| {
                        let cont = sum < *amount;
                        sum += coin.amount;
                        cont
                    })
                    .cloned()
                    .collect();

                dbg!(&selected_coins);
                selected_coins
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
    println!("Use cargo test");
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use itertools::Itertools;
    use rand::seq::SliceRandom;
    use rocksdb::DB;

    use crate::{CoinDef, CoinIndexDef, CoinsManager};

    type UtxoIdDef = str;
    type OwnerDef = str;
    type AssetDef = str;
    type AmountDef = u64;

    pub type Coin = (
        &'static UtxoIdDef,
        &'static OwnerDef,
        &'static AssetDef,
        u64,
    );
    pub type CoinDatabase = &'static [Coin];

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

            ("UTXO80", "Eve", "BTC", 100),
            ("UTXO81", "Eve", "BTC", 75),
            ("UTXO82", "Eve", "BTC", 50),
            ("UTXO83", "Eve", "BTC", 25),
            ("UTXO84", "Eve", "BTC", 5),
            ("UTXO85", "Eve", "BTC", 4),
            ("UTXO86", "Eve", "BTC", 3),
            ("UTXO87", "Eve", "BTC", 2),
            ("UTXO88", "Eve", "BTC", 1),
            ("UTXO90", "Eve", "LCK", 100),
            ("UTXO91", "Eve", "LCK", 75),
            ("UTXO92", "Eve", "LCK", 50),
            ("UTXO93", "Eve", "LCK", 25),
            ("UTXO94", "Eve", "LCK", 5),
            ("UTXO95", "Eve", "LCK", 4),
            ("UTXO96", "Eve", "LCK", 3),
            ("UTXO97", "Eve", "LCK", 2),
            ("UTXO98", "Eve", "LCK", 1),

            ("UTXO11", "Frank", "BTC", 1),
            
            ("UTXO12", "Grace", "BTC", 2),
            ("UTXO13", "Grace", "BTC", 3),
            ("UTXO14", "Grace", "ETH", 1),
            ("UTXO15", "Grace", "ETH", 3),
            ("UTXO16", "Grace", "BTC", 1),
            ("UTXO17", "Grace", "ETH", 2),
        ];

    mod coins {
        use test_case::test_case;

        use crate::{
            make_string_exactly_8_chars_long,
            tests::{assert_coins, make_coin_manager},
            CoinIndexDef,
        };

        use super::{AmountDef, AssetDef, Coin, CoinDatabase, OwnerDef, UtxoIdDef, COIN_DATABASE};

        impl From<&(&UtxoIdDef, &OwnerDef, &AssetDef, AmountDef)> for CoinIndexDef {
            fn from(
                (utxo_id, user, asset, amount): &(&UtxoIdDef, &OwnerDef, &AssetDef, AmountDef),
            ) -> Self {
                Self::new(
                    make_string_exactly_8_chars_long(user.to_string()),
                    make_string_exactly_8_chars_long(asset.to_string()),
                    *amount,
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
            let actual_coins: Vec<_> = cm.coins(owner, asset, NO_EXCLUDED_UTXO_IDS);
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
            let actual_coins: Vec<_> = cm.coins(owner, asset, &excluded_utxo_ids);

            assert_coins(&expected_coins, &actual_coins, &cm.main_db);
        }
    }

    mod coins_to_spend {
        use std::str::FromStr;

        use crate::{tests::assert_coins_to_spend, Query};

        use super::{assert_coins, make_coin_manager, Coin, CoinDatabase, COIN_DATABASE};
        use test_case::test_case;

        #[derive(Debug)]
        struct TestCase {
            coins: CoinDatabase,
            owner: &'static str,
            excluded_utxos: &'static str,
            query: &'static [&'static str],
            expected_coins: &'static [Coin],
        }

        const SINGLE_ASSET_MULTIPLE_COINS_WITHOUT_EXCLUSION: TestCase = TestCase {
            coins: COIN_DATABASE,
            owner: "Eve",
            excluded_utxos: "",
            query: &["BTC;150;-"],
            expected_coins: &[("UTXO80", "Eve", "BTC", 100), ("UTXO81", "Eve", "BTC", 75)],
        };

        const SINGLE_ASSET_MULTIPLE_COINS_WITH_SINGLE_EXCLUSION: TestCase = TestCase {
            coins: COIN_DATABASE,
            owner: "Eve",
            excluded_utxos: "UTXO80",
            query: &["BTC;150;-"],
            expected_coins: &[
                ("UTXO81", "Eve", "BTC", 75),
                ("UTXO82", "Eve", "BTC", 50),
                ("UTXO83", "Eve", "BTC", 25),
            ],
        };

        const SINGLE_ASSET_MULTIPLE_COINS_WITH_MULTIPLE_EXCLUSIONS: TestCase = TestCase {
            coins: COIN_DATABASE,
            owner: "Eve",
            excluded_utxos: "UTXO80;UTXO81;UTXO82;UTXO83;UTXO84;UTXO85",
            query: &["BTC;4;-"],
            expected_coins: &[("UTXO86", "Eve", "BTC", 3), ("UTXO87", "Eve", "BTC", 2)],
        };

        #[test_case(SINGLE_ASSET_MULTIPLE_COINS_WITHOUT_EXCLUSION; "Multiple coins for single asset without excluded UTXOs")]
        #[test_case(SINGLE_ASSET_MULTIPLE_COINS_WITH_SINGLE_EXCLUSION; "Multiple coins for single asset with single exclusion")]
        #[test_case(SINGLE_ASSET_MULTIPLE_COINS_WITH_MULTIPLE_EXCLUSIONS; "Multiple coins for single asset with multiple exclusions")]
        fn exclude_coin(
            TestCase {
                coins,
                owner,
                excluded_utxos,
                query,
                expected_coins,
            }: TestCase,
        ) {
            let excluded_utxo_ids = excluded_utxos
                .split(';')
                .map(Into::into)
                .collect::<Vec<_>>();

            let query: Vec<Query> = query.iter().map(|s| Query::from_str(s).unwrap()).collect();

            dbg!(&query);

            let cm = make_coin_manager(coins);
            let actual_coins: Vec<_> = cm.coins_to_spend(owner, &query, &excluded_utxo_ids);

            assert_coins_to_spend(&expected_coins, &actual_coins, &cm.main_db);
        }
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
            let serialized_metadata = main_db.get(&coin.utxo_id).unwrap().unwrap();
            let deserialized_metadata: CoinDef =
                postcard::from_bytes(&serialized_metadata).unwrap();
            assert!(!deserialized_metadata.metadata.is_empty());
            unique_metadata.insert(deserialized_metadata.metadata.clone());
        }

        // Make sure that metadata are unique, ie. link to main DB is correct even for "identical" coins.
        assert_eq!(unique_metadata.len(), actual.len());
    }

    fn assert_coins_to_spend(expected: &[Coin], actual: &[Vec<CoinIndexDef>], main_db: &DB) {
        let expected_grouped_by_asset = expected
            .iter()
            .map(Into::into)
            .into_group_map_by(|coin: &CoinIndexDef| coin.asset.clone());
        dbg!(&expected_grouped_by_asset);

        let actual_grouped_by_asset = actual
            .iter()
            .map(|coins| coins.iter().cloned())
            .flatten()
            .into_group_map_by(|coin: &CoinIndexDef| coin.asset.clone());
        dbg!(&actual_grouped_by_asset);

        assert_eq!(expected_grouped_by_asset, actual_grouped_by_asset);
    }
}
