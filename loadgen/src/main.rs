use std::{cmp, sync::Arc};

use env_logger::Env;
use log::{error, info, warn};

#[macro_use]
extern crate structopt;
extern crate clap_verbosity_flag;
use stopwatch::Stopwatch;
use structopt::StructOpt;

#[macro_use]
extern crate mongodb;
use bson::Document;
// use bson::doc;
use mongodb::{
    options::{DropCollectionOptions, WriteConcern},
    Client,
};

use anyhow::Result;

use rand::prelude::*;
use rand_pcg::Pcg64;

// use tokio

/// Search for a pattern in a file and display the lines that contain it.
#[derive(Debug, StructOpt)]
#[structopt(global_settings(&[structopt::clap::AppSettings::ColoredHelp]))]
struct CmdLine {
    #[structopt(flatten)]
    verbose: clap_verbosity_flag::Verbosity,

    #[structopt(name = "debug", short = "d", long = "debug")]
    /// Debug output
    debug: bool,

    /// server to load
    #[structopt(long = "uri", default_value = "mongodb://localhost:27017/")]
    uri: String,

    /// Database to load
    #[structopt(long = "database", default_value = "loadgen")]
    database: String,

    /// Collection to load
    #[structopt(short = "c", long = "collection", default_value = "load")]
    collection: String,

    /// Number of Threads to load with
    #[structopt(short = "t", long = "threads", default_value = "0")]
    threads: u16,

    /// Number of Documents to load
    #[structopt(long = "docs", default_value = "10")]
    docs: u64,

    /// Create N collections where N = threads
    #[structopt(name = "shard", long = "shard")]
    shard: bool,

    /// Drop collection first
    #[structopt(name = "drop", long = "drop")]
    drop: bool,
}

// #[tokio::main]
// pub async fn main() -> Result<()> {
//     env_logger::from_env(Env::default().default_filter_or("debug")).init();

//     let args = CmdLine::from_args();

//     println!("Hello, world!");

//         Ok(())q
// }

#[derive(Debug, Clone)]
struct DataGeneratorParams {
    uri: String,

    database: String,

    collection: String,

    // threads : u16,
    docs: u64,
    // shard: bool,
}

impl DataGeneratorParams {
    fn from_args(args: &CmdLine) -> DataGeneratorParams {
        DataGeneratorParams {
            uri: args.uri.clone(),
            database: args.database.clone(),
            collection: args.collection.clone(),
            docs: args.docs,
            // shard : args.shard
        }
    }
}

async fn do_work(args: DataGeneratorParams, offset: u64) -> Result<()> {
    let client = Client::with_uri_str(&args.uri).await?;

    let db = client.database(&args.database);
    for coll_name in db.list_collection_names(None).await? {
        println!("collection: {}", coll_name);
    }

    let mut rng = Pcg64::from_rng(thread_rng())?;

    let coll = db.collection(&args.collection);

    let total_docs = args.docs;

    let batch_size = 100;

    let batches: usize = cmp::max((total_docs as usize) / batch_size, 1);

    let fields: i32 = 10;
    let field_size = 100;

    for i in 0..batches {
        let mut docs: Vec<Document> = Vec::new();
        docs.reserve(batch_size);

        for b in 0..batch_size {
            let mut d = Document::new();
            d.insert(
                "_id",
                ((offset * total_docs) as usize + (i * batch_size) + b) as u64,
            );

            for f in 0..fields {
                let mut buf: Vec<u8> = Vec::new();
                buf.resize(field_size, 0);

                rng.fill_bytes(&mut buf);

                d.insert(
                    format!("field{}", f),
                    bson::Binary {
                        subtype: bson::spec::BinarySubtype::Generic,
                        bytes: buf,
                    },
                );
            }

            docs.push(d);
        }

        coll.insert_many(docs, None).await?;
        // let result = coll.insert_one(d, None).await?;
        // println!("{:#?}", result);
    }

    Ok(())
}

async fn drop_coll(args: Arc<CmdLine>) -> Result<()> {
    let client = Client::with_uri_str(&args.uri).await?;

    let db = client.database(&args.database);

    let coll = db.collection(&args.collection);

    coll.drop(None).await?;

    // coll.drop( DropCollectionOptions::builder()
    // .write_concern(
    //     WriteConcern::builder().build()
    // ).build()).await?;

    Ok(())
}

#[tokio::main]
pub async fn main() {
    env_logger::from_env(Env::default().default_filter_or("debug")).init();

    let args = Arc::new(CmdLine::from_args());

    println!("Hello, world!");

    if args.drop {
        let a1 = args.clone();
        let r = drop_coll(a1).await;
        if r.is_err() {
            error!("Do drop error: {:?}", r);
        }
    }

    let mut tasks = Vec::new();

    let sw = Stopwatch::start_new();

    for i in 0..args.threads {
        let a1 = args.clone();
        let offset: u64 = (i as u64) * args.docs;
        let handle = tokio::spawn(async move {
            // Do some async work

            let mut dg = DataGeneratorParams::from_args(a1.as_ref());

            if a1.shard {
                dg.collection = format!("load{}", i);
            }

            let r = do_work(dg, offset).await;
            if r.is_err() {
                error!("Do work error: {:?}", r);
            }
        });

        tasks.push(handle);
    }

    // Do some other work

    for handle in tasks {
        let out = handle.await.unwrap();
        println!("GOT {:?}", out);
    }

    let elapsed = sw.elapsed();

    println!("Load Duration: {:?}", elapsed);
}

/*
TODO
File bug?
I saw this line on my debug log from the 2.0.0-alpha driver on my Fedora 31 machine.
 My machine does not have the `lsb_release` executable.
 ```[2021-02-09T19:15:35Z DEBUG os_info::imp::lsb_release] lsb_release command failed with Os { code: 2, kind: NotFound, message: "No such file or directory" }```

File bug?

// Bad write concern generated
    // coll.drop( DropCollectionOptions::builder()
    // .write_concern(
    //     WriteConcern::builder().build()
    // ).build()).await?;
*/
