use rand::seq::SliceRandom;
use rocksdb::{DBCommon, Options, SingleThreaded, ThreadMode, DB};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug)]
struct Key {
    amount: u64,
}

impl Key {
    fn new(amount: u64) -> Self {
        Self { amount }
    }

    fn as_be_bytes(&self) -> [u8; 8] {
        self.amount.to_be_bytes()
    }

    fn from_be_bytes(bytes: [u8; 8]) -> Self {
        Self {
            amount: u64::from_be_bytes(bytes),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct Data {
    user: [u8; 32],
    asset: [u8; 32],
}

impl Data {
    fn new(user: u8) -> Self {
        Self {
            user: [user; 32],
            asset: [user; 32],
        }
    }
}

struct Obj {
    key: Key,
    data: Data,
}

impl Obj {
    fn new(key: Key, data: Data) -> Self {
        Self { key, data }
    }
}

fn main() {
    // Configure RocksDB options
    let mut options = Options::default();
    options.create_if_missing(true);

    // Open a RocksDB database
    let db = DB::open(&options, "simple_db").unwrap();

    let mut random_amounts = std::iter::repeat_with(rand::random::<u64>)
        .take(50)
        .collect::<Vec<_>>();
    random_amounts.sort();
    let sorted_amounts = random_amounts.clone();
    let mut sorted_amounts_with_order = sorted_amounts
        .iter()
        .enumerate()
        .map(|(i, amount)| (*amount, i as u8))
        .collect::<Vec<_>>();
    sorted_amounts_with_order.shuffle(&mut rand::thread_rng());
    sorted_amounts_with_order
        .iter()
        .for_each(|(amount, order)| {
            println!("Putting {amount} on place {order}");
            let o = Obj::new(Key::new(*amount as u64), Data::new(*order as u8));
            put_some(&db, &o);
        });

    // Iterate over all entries
    println!("Iterating over RocksDB entries:");
    let iter = db.iterator(rocksdb::IteratorMode::Start);
    for x in iter {
        let (key_bytes, value) = x.unwrap();
        let key = Key::from_be_bytes(postcard::from_bytes(&key_bytes).unwrap());
        let Data { user, .. } = postcard::from_bytes(&value).unwrap();
        println!(
            "Amount: {:?}, Ordering: {:?}     -     key_bytes={}",
            key.amount,
            user[0],
            hex::encode(&key_bytes)
        );
    }

    // Clean up the database files after the example (optional)
    drop(db);
    let _ = DB::destroy(&options, "simple_db");
}

fn put_some(db: &DB, /*amount: u64, user: u8, asset: u8*/ o: &Obj) {
    let key = o.key.clone();
    let data = o.data.clone();

    let key = Key { amount: key.amount };
    let data = Data {
        user: [data.user[0]; 32],
        asset: [data.asset[0]; 32],
    };

    // Serialize Key and Data to binary format
    let serialized_key = postcard::to_allocvec(&key.as_be_bytes()).unwrap();
    let serialized_data = postcard::to_allocvec(&data).unwrap();

    // Store serialized data in RocksDB
    db.put(serialized_key, serialized_data).unwrap();
}
