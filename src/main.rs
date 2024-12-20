use std::str::FromStr;

use rocksdb::{Options, SliceTransform, DB};
use serde::{Deserialize, Serialize};

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
    #[allow(dead_code)]
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

#[allow(dead_code)]
struct CoinsManager {
    main_db: DB,
    index_db: DB,
}

#[derive(Debug)]
#[allow(dead_code)]
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

#[allow(dead_code)]
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
        self.index_db.put(key, []).unwrap();
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
        let eligible_coins = q
            .iter()
            .map(|q| self.coins(&user, &q.asset, excluded_utxo_ids));

        q.iter()
            .zip(eligible_coins)
            .map(|(Query { amount, max, .. }, eligible_coins)| {
                let mut value_accumulated = 0;
                let mut coins_taken = 0;
                let max = max.unwrap_or(u32::MAX) as usize;
                let mut selected_coins: Vec<_> = eligible_coins
                    .iter()
                    .rev()
                    .take(max)
                    .take_while(|coin| {
                        // TODO[RC]: Refactor and simplify this
                        let should_continue = value_accumulated < *amount;
                        value_accumulated += coin.amount;
                        coins_taken += 1;
                        should_continue
                    })
                    .cloned()
                    .collect();

                if value_accumulated >= *amount && coins_taken <= max {
                    let slots_left = max - selected_coins.len();
                    let to_be_filled_with_dust =
                        slots_left.min(eligible_coins.len() - selected_coins.len());
                    selected_coins
                        .extend(eligible_coins.iter().take(to_be_filled_with_dust).cloned());
                    selected_coins
                } else {
                    vec![]
                }
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
    use std::collections::BTreeSet;

    use itertools::Itertools;
    use rand::{rngs::ThreadRng, seq::SliceRandom, Rng};
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
            assert_coins(expected_coins, &actual_coins, &cm.main_db);
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

            assert_coins(expected_coins, &actual_coins, &cm.main_db);
        }
    }

    mod coins_to_spend {
        use std::str::FromStr;

        use crate::{tests::assert_coins_to_spend, Query};

        use super::{make_coin_manager, Coin, CoinDatabase};
        use test_case::test_case;

        #[derive(Debug)]
        struct TestCase {
            coins: CoinDatabase,
            owner: &'static str,
            excluded_utxos: &'static str,
            query: &'static [&'static str],
            expected_coins: &'static [Coin],
        }

        mod without_exclusion_without_limits {
            use crate::tests::COIN_DATABASE;

            use super::TestCase;

            pub(super) const SINGLE_ASSET: TestCase = TestCase {
                coins: COIN_DATABASE,
                owner: "Eve",
                excluded_utxos: "",
                query: &["BTC;150;-"],
                expected_coins: &[
                    ("UTXO80", "Eve", "BTC", 100),
                    ("UTXO81", "Eve", "BTC", 75),
                    ("UTXO88", "Eve", "BTC", 1),
                    ("UTXO87", "Eve", "BTC", 2),
                    ("UTXO86", "Eve", "BTC", 3),
                    ("UTXO85", "Eve", "BTC", 4),
                    ("UTXO84", "Eve", "BTC", 5),
                    ("UTXO83", "Eve", "BTC", 25),
                    ("UTXO82", "Eve", "BTC", 50),
                ],
            };

            pub(super) const MULTIPLE_ASSETS: TestCase = TestCase {
                coins: COIN_DATABASE,
                owner: "Eve",
                excluded_utxos: "",
                query: &["BTC;150;-", "LCK;99;-"],
                expected_coins: &[
                    ("UTXO80", "Eve", "BTC", 100),
                    ("UTXO81", "Eve", "BTC", 75),
                    ("UTXO88", "Eve", "BTC", 1),
                    ("UTXO87", "Eve", "BTC", 2),
                    ("UTXO86", "Eve", "BTC", 3),
                    ("UTXO85", "Eve", "BTC", 4),
                    ("UTXO84", "Eve", "BTC", 5),
                    ("UTXO83", "Eve", "BTC", 25),
                    ("UTXO82", "Eve", "BTC", 50),
                    ("UTXO90", "Eve", "LCK", 100),
                    ("UTXO98", "Eve", "LCK", 1),
                    ("UTXO97", "Eve", "LCK", 2),
                    ("UTXO96", "Eve", "LCK", 3),
                    ("UTXO95", "Eve", "LCK", 4),
                    ("UTXO94", "Eve", "LCK", 5),
                    ("UTXO93", "Eve", "LCK", 25),
                    ("UTXO92", "Eve", "LCK", 50),
                    ("UTXO91", "Eve", "LCK", 75),
                ],
            };
        }

        mod with_exclusion_without_limits {
            use crate::tests::COIN_DATABASE;

            use super::TestCase;

            pub(super) const SINGLE_EXCLUSION: TestCase = TestCase {
                coins: COIN_DATABASE,
                owner: "Eve",
                excluded_utxos: "UTXO80",
                query: &["BTC;150;-"],
                expected_coins: &[
                    ("UTXO81", "Eve", "BTC", 75),
                    ("UTXO82", "Eve", "BTC", 50),
                    ("UTXO83", "Eve", "BTC", 25),
                    ("UTXO88", "Eve", "BTC", 1),
                    ("UTXO87", "Eve", "BTC", 2),
                    ("UTXO86", "Eve", "BTC", 3),
                    ("UTXO85", "Eve", "BTC", 4),
                    ("UTXO84", "Eve", "BTC", 5),
                ],
            };

            pub(super) const MULTIPLE_EXCLUSIONS: TestCase = TestCase {
                coins: COIN_DATABASE,
                owner: "Eve",
                excluded_utxos: "UTXO80;UTXO81;UTXO82;UTXO83",
                query: &["BTC;10;-"],
                expected_coins: &[
                    ("UTXO84", "Eve", "BTC", 5),
                    ("UTXO85", "Eve", "BTC", 4),
                    ("UTXO86", "Eve", "BTC", 3),
                    ("UTXO88", "Eve", "BTC", 1),
                    ("UTXO87", "Eve", "BTC", 2),
                ],
            };

            pub(super) const MULTIPLE_ASSETS: TestCase = TestCase {
                coins: COIN_DATABASE,
                owner: "Eve",
                excluded_utxos: "UTXO90",
                query: &["BTC;200;-", "LCK;100;-"],
                expected_coins: &[
                    ("UTXO80", "Eve", "BTC", 100),
                    ("UTXO81", "Eve", "BTC", 75),
                    ("UTXO82", "Eve", "BTC", 50),
                    ("UTXO88", "Eve", "BTC", 1),
                    ("UTXO87", "Eve", "BTC", 2),
                    ("UTXO86", "Eve", "BTC", 3),
                    ("UTXO85", "Eve", "BTC", 4),
                    ("UTXO84", "Eve", "BTC", 5),
                    ("UTXO83", "Eve", "BTC", 25),
                    ("UTXO91", "Eve", "LCK", 75),
                    ("UTXO92", "Eve", "LCK", 50),
                    ("UTXO98", "Eve", "LCK", 1),
                    ("UTXO97", "Eve", "LCK", 2),
                    ("UTXO96", "Eve", "LCK", 3),
                    ("UTXO95", "Eve", "LCK", 4),
                    ("UTXO94", "Eve", "LCK", 5),
                    ("UTXO93", "Eve", "LCK", 25),
                ],
            };
        }

        mod without_exclusion_with_limits {
            use crate::tests::COIN_DATABASE;

            use super::TestCase;

            pub(super) const SINGLE_ASSET_PERFECT_FIT: TestCase = TestCase {
                coins: COIN_DATABASE,
                owner: "Eve",
                excluded_utxos: "",
                query: &["BTC;225;3"],
                expected_coins: &[
                    ("UTXO80", "Eve", "BTC", 100),
                    ("UTXO81", "Eve", "BTC", 75),
                    ("UTXO82", "Eve", "BTC", 50),
                ],
            };

            pub(super) const SINGLE_ASSET_LACK_SINGLE_COIN: TestCase = TestCase {
                coins: COIN_DATABASE,
                owner: "Eve",
                excluded_utxos: "UTXO80",
                query: &["BTC;225;2"],
                expected_coins: &[],
            };

            pub(super) const MULTIPLE_ASSETS_PERFECT_FIT: TestCase = TestCase {
                coins: COIN_DATABASE,
                owner: "Eve",
                excluded_utxos: "",
                query: &["BTC;225;3", "LCK;250;4"],
                expected_coins: &[
                    ("UTXO80", "Eve", "BTC", 100),
                    ("UTXO81", "Eve", "BTC", 75),
                    ("UTXO82", "Eve", "BTC", 50),
                    ("UTXO90", "Eve", "LCK", 100),
                    ("UTXO91", "Eve", "LCK", 75),
                    ("UTXO92", "Eve", "LCK", 50),
                    ("UTXO93", "Eve", "LCK", 25),
                ],
            };

            pub(super) const NOT_ALL_ASSETS_LIMITED: TestCase = TestCase {
                coins: COIN_DATABASE,
                owner: "Eve",
                excluded_utxos: "",
                query: &["BTC;225;2", "LCK;250;-"],
                expected_coins: &[
                    ("UTXO90", "Eve", "LCK", 100),
                    ("UTXO91", "Eve", "LCK", 75),
                    ("UTXO92", "Eve", "LCK", 50),
                    ("UTXO93", "Eve", "LCK", 25),
                    ("UTXO98", "Eve", "LCK", 1),
                    ("UTXO97", "Eve", "LCK", 2),
                    ("UTXO96", "Eve", "LCK", 3),
                    ("UTXO95", "Eve", "LCK", 4),
                    ("UTXO94", "Eve", "LCK", 5),
                ],
            };
        }

        mod with_exclusion_with_limits {
            use crate::tests::COIN_DATABASE;

            use super::TestCase;

            pub(super) const LIMIT_TOO_LOW_ON_ALL_ASSETS: TestCase = TestCase {
                coins: COIN_DATABASE,
                owner: "Eve",
                excluded_utxos: "UTXO80",
                query: &["BTC;156;3", "LCK;101;1"],
                expected_coins: &[],
            };

            pub(super) const LIMIT_TOO_LOW_ON_SINGLE_ASSET_1: TestCase = TestCase {
                coins: COIN_DATABASE,
                owner: "Eve",
                excluded_utxos: "UTXO80",
                query: &["BTC;156;5", "LCK;101;1"],
                expected_coins: &[
                    ("UTXO81", "Eve", "BTC", 75),
                    ("UTXO82", "Eve", "BTC", 50),
                    ("UTXO83", "Eve", "BTC", 25),
                    ("UTXO84", "Eve", "BTC", 5),
                    ("UTXO85", "Eve", "BTC", 4),
                ],
            };

            pub(super) const LIMIT_TOO_LOW_ON_SINGLE_ASSET_2: TestCase = TestCase {
                coins: COIN_DATABASE,
                owner: "Eve",
                excluded_utxos: "UTXO80",
                query: &["BTC;156;3", "LCK;101;2"],
                expected_coins: &[("UTXO90", "Eve", "LCK", 100), ("UTXO91", "Eve", "LCK", 75)],
            };
        }

        mod dust_coins {
            use crate::tests::COIN_DATABASE;

            use super::TestCase;

            pub(super) const FILLS_WITH_DUST_COINS_UP_TO_THE_LIMIT: TestCase = TestCase {
                coins: COIN_DATABASE,
                owner: "Eve",
                excluded_utxos: "",
                query: &["BTC;200;6", "LCK;100;2"],
                expected_coins: &[
                    ("UTXO80", "Eve", "BTC", 100),
                    ("UTXO81", "Eve", "BTC", 75),
                    ("UTXO82", "Eve", "BTC", 50),
                    ("UTXO88", "Eve", "BTC", 1),
                    ("UTXO87", "Eve", "BTC", 2),
                    ("UTXO86", "Eve", "BTC", 3),
                    ("UTXO90", "Eve", "LCK", 100),
                    ("UTXO98", "Eve", "LCK", 1),
                ],
            };

            pub(super) const FILLING_WITH_DUST_DOES_NOT_PRODUCE_DUPLICATES: TestCase = TestCase {
                coins: COIN_DATABASE,
                owner: "Eve",
                excluded_utxos: "",
                query: &["LCK;200;-"],
                expected_coins: &[
                    ("UTXO90", "Eve", "LCK", 100),
                    ("UTXO91", "Eve", "LCK", 75),
                    ("UTXO92", "Eve", "LCK", 50),
                    ("UTXO98", "Eve", "LCK", 1),
                    ("UTXO97", "Eve", "LCK", 2),
                    ("UTXO96", "Eve", "LCK", 3),
                    ("UTXO95", "Eve", "LCK", 4),
                    ("UTXO94", "Eve", "LCK", 5),
                    ("UTXO93", "Eve", "LCK", 25),
                ],
            };

            pub(super) const ZERO_DUST_IF_ALL_COINS_USED: TestCase = TestCase {
                coins: COIN_DATABASE,
                owner: "Eve",
                excluded_utxos: "",
                query: &["BTC;265;-"],
                expected_coins: &[
                    ("UTXO80", "Eve", "BTC", 100),
                    ("UTXO81", "Eve", "BTC", 75),
                    ("UTXO82", "Eve", "BTC", 50),
                    ("UTXO83", "Eve", "BTC", 25),
                    ("UTXO84", "Eve", "BTC", 5),
                    ("UTXO85", "Eve", "BTC", 4),
                    ("UTXO86", "Eve", "BTC", 3),
                    ("UTXO87", "Eve", "BTC", 2),
                    ("UTXO88", "Eve", "BTC", 1),
                ],
            };

            pub(super) const ZERO_DUST_IF_ALL_SLOTS_FILLED: TestCase = TestCase {
                coins: COIN_DATABASE,
                owner: "Eve",
                excluded_utxos: "",
                query: &["BTC;225;3"],
                expected_coins: &[
                    ("UTXO80", "Eve", "BTC", 100),
                    ("UTXO81", "Eve", "BTC", 75),
                    ("UTXO82", "Eve", "BTC", 50),
                ],
            };

            pub(super) const NO_EXCLUDED_COINS_IN_DUST: TestCase = TestCase {
                coins: COIN_DATABASE,
                owner: "Eve",
                excluded_utxos: "UTXO98;UTXO95",
                query: &["LCK;200;-"],
                expected_coins: &[
                    ("UTXO90", "Eve", "LCK", 100),
                    ("UTXO91", "Eve", "LCK", 75),
                    ("UTXO92", "Eve", "LCK", 50),
                    ("UTXO97", "Eve", "LCK", 2),
                    ("UTXO96", "Eve", "LCK", 3),
                    ("UTXO94", "Eve", "LCK", 5),
                    ("UTXO93", "Eve", "LCK", 25),
                ],
            };
        }

        #[test_case(without_exclusion_without_limits::SINGLE_ASSET)]
        #[test_case(without_exclusion_without_limits::MULTIPLE_ASSETS)]
        #[test_case(with_exclusion_without_limits::SINGLE_EXCLUSION)]
        #[test_case(with_exclusion_without_limits::MULTIPLE_EXCLUSIONS)]
        #[test_case(with_exclusion_without_limits::MULTIPLE_ASSETS)]
        #[test_case(without_exclusion_with_limits::SINGLE_ASSET_PERFECT_FIT)]
        #[test_case(without_exclusion_with_limits::SINGLE_ASSET_LACK_SINGLE_COIN)]
        #[test_case(without_exclusion_with_limits::MULTIPLE_ASSETS_PERFECT_FIT)]
        #[test_case(without_exclusion_with_limits::NOT_ALL_ASSETS_LIMITED)]
        #[test_case(with_exclusion_with_limits::LIMIT_TOO_LOW_ON_ALL_ASSETS)]
        #[test_case(with_exclusion_with_limits::LIMIT_TOO_LOW_ON_SINGLE_ASSET_1)]
        #[test_case(with_exclusion_with_limits::LIMIT_TOO_LOW_ON_SINGLE_ASSET_2)]
        #[test_case(dust_coins::FILLS_WITH_DUST_COINS_UP_TO_THE_LIMIT)]
        #[test_case(dust_coins::FILLING_WITH_DUST_DOES_NOT_PRODUCE_DUPLICATES)]
        #[test_case(dust_coins::ZERO_DUST_IF_ALL_COINS_USED)]
        #[test_case(dust_coins::ZERO_DUST_IF_ALL_SLOTS_FILLED)]
        #[test_case(dust_coins::NO_EXCLUDED_COINS_IN_DUST)]

        fn selected_coins(
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

            assert_coins_to_spend(expected_coins, &actual_coins, &cm.main_db);
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

    fn assert_coins_to_spend(expected: &[Coin], actual: &[Vec<CoinIndexDef>], _main_db: &DB) {
        let expected_grouped_by_asset = expected
            .iter()
            .map(Into::into)
            .into_group_map_by(|coin: &CoinIndexDef| coin.asset.clone());
        dbg!(&expected_grouped_by_asset);

        let actual_grouped_by_asset = actual
            .iter()
            .flat_map(|coins| coins.iter().cloned())
            .into_group_map_by(|coin: &CoinIndexDef| coin.asset.clone());
        dbg!(&actual_grouped_by_asset);

        assert_eq!(expected_grouped_by_asset, actual_grouped_by_asset);
    }

    // fn select_coins<'a, I, J>(
    //     mut smallest: I,
    //     mut biggest: J,
    //     total: u64,
    //     max: u8,
    //     rng: &mut ThreadRng,
    // ) -> Option<()>
    // where
    //     I: Iterator<Item = &'a u64>,
    //     J: Iterator<Item = &'a u64>,
    // {
    //     let mut sum = 0;
    //     let mut count = 0;
    //     let big_end = loop {
    //         let maybe_biggest_num = biggest.next();
    //         match maybe_biggest_num {
    //             Some(biggest_num) => {
    //                 sum += biggest_num;
    //                 count += 1;
    //                 println!(
    //                     "Added biggest: {}, total={}/{total}, count={}/{max}",
    //                     biggest_num, sum, count
    //                 );
    //                 if sum >= total {
    //                     break Some(biggest);
    //                 }
    //             }
    //             None => break None,
    //         }
    //     };

    //     if sum < total {
    //         // Unable to satisfy the total amount with all coins.
    //         return None;
    //     }

    //     let free_slots = max - count;
    //     let to_be_filled_with_dust = rng.gen_range(2..free_slots);
    //     println!(
    //         "Free slots: {}. I will fill {} of them with dust",
    //         free_slots, to_be_filled_with_dust
    //     );

    //     let small_end = if to_be_filled_with_dust > 0 {
    //         let mut dust_sum = 0;
    //         let mut dust_count = 0;
    //         loop {
    //             let maybe_smallest_num = smallest.next();
    //             match maybe_smallest_num {
    //                 Some(smallest_num) => {
    //                     dust_count += 1;
    //                     dust_sum += smallest_num;
    //                     println!("Added smallest: {}, total = {dust_sum}", smallest_num);
    //                     if dust_count >= to_be_filled_with_dust {
    //                         break Some(smallest);
    //                     }
    //                 }
    //                 None => break None,
    //             }
    //         }
    //     } else {
    //         None
    //     };

    //     todo!()
    // }

    // fn take_dust<'a, I>(mut coins_iter: I, stop_at: u64, max: u8) -> impl Iterator<Item = &'a u64>
    // where
    //     I: Iterator<Item = &'a u64>,
    // {
    //     coins_iter
    //         .take_while(move |item| **item != stop_at)
    //         .take(max as usize)
    // }

    /*
    fn select<'a, I>(
        coins_iter: I,
        total: u64,
        max: u8,
        rng: &mut ThreadRng,
    ) -> impl Iterator<Item = u64>
    where
        I: DoubleEndedIterator<Item = &'a u64> + Clone,
    {
        let big_coins: Vec<_> = take_bigs(coins_iter.clone(), total, max).collect();
        println!("Big coins: {:?}", big_coins);
        let last_big_coin = big_coins.last().unwrap();
        println!("Last big coin: {}", last_big_coin);
        let free_slots = max - big_coins.len() as u8;
        println!("Free slots: {}", free_slots);
        let max_dust_count = rng.gen_range(1..free_slots);
        //let max_dust_count = free_slots;
        println!("Will get at most {max_dust_count} dust coins");
        let small_coins: Vec<_> = take_dust(coins_iter.rev(), **last_big_coin, max_dust_count)
            .map(|x| *x)
            .collect();
        println!("Small coins: {:?}", small_coins);
        let mut sum_of_small_coins: u64 = small_coins.iter().sum();
        println!("Sum of small coins: {}", sum_of_small_coins);
        let adjusted_big_coins: Vec<_> = big_coins
            .iter()
            .skip_while(|item| {
                let maybe_new_value = sum_of_small_coins.checked_sub(***item);
                match maybe_new_value {
                    Some(new_value) => {
                        sum_of_small_coins = new_value;
                        true
                    }
                    None => false,
                }
            })
            .map(|x| **x)
            .collect();
        println!("Adjusted big coins: {:?}", adjusted_big_coins);
        adjusted_big_coins
            .into_iter()
            .chain(small_coins.into_iter())
    }
    */

    fn select_1<'a, I, J>(
        coins_iter: I,
        coins_iter_back: J,
        total: u128,
        max: u8,
        excluded: &[u64],
        rng: &mut ThreadRng,
    ) -> Box<dyn Iterator<Item = &'a u64> + 'a>
    where
        I: Iterator<Item = &'a u64> + 'a,
        J: Iterator<Item = &'a u64> + 'a,
    {
        if total == 0 && max == 0 {
            return Box::new(std::iter::empty());
        }

        let (big_coins_total, big_coins) = big_coins(coins_iter, total, max, excluded);
        if big_coins_total < total {
            return Box::new(std::iter::empty());
        }
        let Some(last_selected_big_coin) = big_coins.last() else {
            // Should never happen.
            return Box::new(std::iter::empty());
        };

        let max_dust_count = max_dust_count(max, &big_coins, rng);
        let (dust_coins_total, dust_coins) = dust_coins(
            coins_iter_back,
            last_selected_big_coin,
            max_dust_count,
            excluded,
        );

        let retained_big_coins_iter = skip_big_coins_up_to_amount(big_coins, dust_coins_total);
        Box::new(retained_big_coins_iter.chain(dust_coins))
    }

    fn skip_big_coins_up_to_amount<'a>(
        big_coins: impl IntoIterator<Item = &'a u64>,
        mut dust_coins_total: u128,
    ) -> impl Iterator<Item = &'a u64> {
        big_coins.into_iter().skip_while(move |item| {
            dust_coins_total
                .checked_sub(**item as u128)
                .and_then(|new_value| {
                    dust_coins_total = new_value;
                    Some(true)
                })
                .unwrap_or_default()
        })
    }

    fn dust_coins<'a, I>(
        coins_iter_back: I,
        last_big_coin: &'a u64,
        max_dust_count: u8,
        excluded: &[u64],
    ) -> (u128, Vec<&'a u64>)
    where
        I: Iterator<Item = &'a u64> + 'a,
    {
        let mut dust_coins_total = 0;
        let dust_coins: Vec<&u64> = coins_iter_back
            .filter(|item| !excluded.contains(item))
            .take(max_dust_count as usize)
            .take_while(move |item| *item != last_big_coin)
            .map(|item| {
                dust_coins_total += *item as u128;
                item
            })
            .collect();
        (dust_coins_total, dust_coins)
    }

    fn max_dust_count(max: u8, big_coins: &Vec<&u64>, rng: &mut ThreadRng) -> u8 {
        rng.gen_range(0..=max.saturating_sub(big_coins.len() as u8))
    }

    fn big_coins<'a, I>(
        coins_iter: I,
        total: u128,
        max: u8,
        excluded: &[u64],
    ) -> (u128, Vec<&'a u64>)
    where
        I: Iterator<Item = &'a u64> + 'a,
    {
        let mut big_coins_total = 0;
        let big_coins: Vec<&u64> = coins_iter
            .filter(|item| !excluded.contains(item))
            .take(max as usize)
            .take_while(|item| {
                (big_coins_total >= total)
                    .then_some(false)
                    .unwrap_or_else(|| {
                        big_coins_total += **item as u128;
                        true
                    })
            })
            .collect();
        (big_coins_total, big_coins)
    }

    #[test]
    fn selection_algo_exact() {
        let coins = [15, 10, 8, 7, 6, 5, 4, 3, 2, 1];
        let mut rng = rand::thread_rng();

        let total = 61;
        let max = 10;
        let excluded = [];

        let result: Vec<_> = select_1(
            coins.iter(),
            coins.iter().rev(),
            total,
            max,
            &excluded,
            &mut rng,
        )
        .collect();
        dbg!(&result);
    }

    #[test]
    fn selection_algo() {
        let coins = [15, 10, 8, 7, 6, 5, 4, 3, 2, 1];
        let mut rng = rand::thread_rng();

        let total = 25;
        let max = 7;
        let excluded = [1, 2, 10];

        let result: Vec<_> = select_1(
            coins.iter(),
            coins.iter().rev(),
            total,
            max,
            &excluded,
            &mut rng,
        )
        .collect();
        dbg!(&result);
    }

    #[test]
    fn selection_algo_1() {
        let coins = [15, 10, 8, 7, 6, 5, 4, 3, 2, 1];
        let mut rng = rand::thread_rng();

        let total = 25;
        let max = 4;
        let excluded = [];

        let result: Vec<_> = select_1(
            coins.iter(),
            coins.iter().rev(),
            total,
            max,
            &excluded,
            &mut rng,
        )
        .collect();
        dbg!(&result);
    }

    #[test]
    fn selection_algo_can_not_satisfy() {
        let coins = [15, 10, 8, 7, 6, 5, 4, 3, 2, 1];
        let mut rng = rand::thread_rng();

        let total = 20000;
        let max = 13;
        let excluded = [];

        let result: Vec<_> = select_1(
            coins.iter(),
            coins.iter().rev(),
            total,
            max,
            &excluded,
            &mut rng,
        )
        .collect();
        dbg!(&result);
    }

    #[test]
    fn selection_algo_no_coins() {
        let coins = [];
        let mut rng = rand::thread_rng();

        let total = 1;
        let max = 13;
        let excluded = [];

        let result: Vec<_> = select_1(
            coins.iter(),
            coins.iter().rev(),
            total,
            max,
            &excluded,
            &mut rng,
        )
        .collect();
        dbg!(&result);
    }

    fn find_subset_sum<'a>(
        coins: impl Iterator<Item = &'a u64>,
        target: u64,
        max: u8,
        rng: &mut ThreadRng,
    ) -> (impl Iterator<Item = &'a u64>, impl Iterator<Item = &'a u64>) {
        // Accumulate sum of first elements
        let mut current_sum = 0;

        let mut big = Vec::with_capacity(max as usize);
        let mut dust = Vec::with_capacity(max as usize);
        let mut switched = false;
        let mut dust_value = 0;
        let mut dust_count = 0;
        let mut max_dust_count = 0;
        let mut last_big = 0;

        for coin in coins.take(max as usize) {
            current_sum += coin;
            if switched {
                if dust_count == max_dust_count || coin == &last_big {
                    break;
                }
                dust_value += coin;
                dust_count += 1;
                dust.push(coin);
            } else {
                big.push(coin);
                if current_sum >= target {
                    switched = true;
                    last_big = *coin;
                    max_dust_count = rng.gen_range(0..=max as usize - big.len());
                }
            }
        }

        (
            dust.into_iter().rev(),
            big.into_iter().take(max as usize).skip_while(move |item| {
                dust_value
                    .checked_sub(**item)
                    .and_then(|new_value| {
                        dust_value = new_value;
                        Some(true)
                    })
                    .unwrap_or_default()
            }),
        )
    }

    #[test]
    fn selection_algo_partition() {
        let mut rng = rand::thread_rng();

        let coins = [15, 10, 8, 7, 6, 5, 4, 3, 2, 1];

        let total = 25;
        let max = 7;

        let (dust_iter, big_iter) = find_subset_sum(coins.iter(), total, max, &mut rng);

        let dust: Vec<_> = dust_iter.take(max as usize).collect();
        let big: Vec<_> = big_iter.collect();

        println!("Big coins: {:?}", big.iter().join(","));
        println!("Dust coins: {:?}", dust.iter().join(","));

        let total_selected_value = big.into_iter().chain(dust).sum::<u64>();
        println!("Total selected value: {}", total_selected_value);
    }
}
