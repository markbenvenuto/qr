use actix_web::{get,post,web, App, HttpRequest, HttpServer, HttpResponse, Responder};

use actix_web::web::Bytes;
use serde::Deserialize;

use actix_web::middleware::Logger;
use env_logger::Env;
use log::{info, warn};

// use bytes::Bytes;

// use std::{fmt::Error, hash::Hash, io::Cursor};
use bson::{Array, Bson, Document};
use bson::doc;

use std::fs::{File, OpenOptions};
// use std::io::{Write, BufReader, BufRead};

use std::io;
use std::fs::{self, DirEntry};
use std::path::Path;
// use std::path::PathBuf;
use lazy_static::lazy_static;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::Arc;
use std::os::unix::prelude::FileExt;
use dashmap::{DashMap};
use actix_http;

use tokio::time::{delay_for, Duration};
use urandom::distributions::{Distribution, Uniform};
// use urandom::urandom;
use urandom::Random;

#[macro_use]
extern crate structopt;
extern crate clap_verbosity_flag;
use structopt::StructOpt;

#[macro_use]
extern crate log;

lazy_static! {
    static ref ROOT_PATH: String = "/data/db/".to_string();
    static ref ROOT_PREFIX: String = "qr/".to_string();
    static ref FILE_MAP: DashMap<String, Arc<RwLock<std::fs::File>>> = DashMap::new();

    static ref GLOBAL_RAND: Mutex< Random<urandom::rng::SplitMix64>> = Mutex::new(urandom::rng::SplitMix64::new());
    static ref GLOBAL_DIST: Mutex< Uniform<u64>> = Mutex::new(Uniform::from(100..1000));
}


async fn greet(req: HttpRequest) -> impl Responder {
    let name = req.match_info().get("name").unwrap_or("World");
    format!("Hello {}!", &name)
}

//
// const std::string url = str::stream()
// << "http://" << _apiUrl << "/os_read?snapshotId=" << _snapshotId << "&filename=" << path
// << "&offset=" << offset << "&length=" << count;

#[derive(Deserialize)]
struct OsReadInfo {
    #[serde(rename="snapshotId")]
    snapshot_id:String,
    filename:String,
    offset:i64,
    length:i64,
}

// MongoDB 4.0 protocol
#[get("/os_read")]
async fn os_read_40(info: web::Query<OsReadInfo>) -> impl Responder {


    // let fh = Arc::new(Mutex::new(file.unwrap()));
    let fh_opt =FILE_MAP.get(&info.filename).map(|x| x.clone());
    let fh = if fh_opt.is_none() {
        warn!("Cannot find in map {:?}", info.filename);

        let full_path = Path::new(&ROOT_PATH.as_str()).join(&info.filename);

        // Wiredtiger opens the journal as a "file", just ignore WT
        if full_path.is_dir() {
            warn!("Opening directory {:?}", full_path);
            return HttpResponse::Ok().finish();
        }

        if !full_path.exists() {
            warn!("Creating file {:?}", full_path);
        }

        info!("Opened file: {:?} at '{:?}'", info.filename, full_path);

        let file = OpenOptions::new().read(true).write(true).create(true).open(full_path);
        if file.is_err() {
            warn!("Cannot open file {:?}", file);
            return HttpResponse::InternalServerError().finish();
        }

        let f2 = file.unwrap();
        println!("len: {:?}", f2.metadata().unwrap().len());

        let fh = Arc::new(RwLock::new(f2));
        FILE_MAP.insert(info.filename.to_string(), fh.clone());
        fh
    } else {
         fh_opt.unwrap()
    };
    // fh.

    let mut buf = Vec::new();
    buf.resize(info.length as usize, 0);

    let off = info.offset as u64;
    let guard = fh.read().unwrap();

    let r = guard.read_at(&mut buf, off);
    if r.is_err() {
        warn!("Cannot read file {:?}", r);
        return HttpResponse::InternalServerError().finish();
    }

    let rs = r.unwrap();
    // info!("Read {:?} bytes", rs);

    // let delay_mills: u64 = {
    // let mut rand =GLOBAL_RAND.lock().unwrap();
    // GLOBAL_DIST.lock().unwrap().sample(&mut *rand)
    // };
    // delay_for(Duration::from_millis(delay_mills)).await;

    HttpResponse::Ok().body(web::Bytes::copy_from_slice(buf.as_slice()))
}


// MongoDB 4.4 protocol
#[get("/os_read")]
async fn os_read_44(info: web::Query<OsReadInfo>) -> impl Responder {


    // let fh = Arc::new(Mutex::new(file.unwrap()));
    let fh_opt =FILE_MAP.get(&info.filename).map(|x| x.clone());
    if fh_opt.is_none() {
        warn!("Cannot find in map {:?}", info.filename);
        return HttpResponse::InternalServerError().finish();
    }
    let fh = fh_opt.unwrap();

    // fh.


    let mut buf = Vec::new();
    buf.resize(info.length as usize, 0);

    let off = info.offset as u64;
    let guard = fh.read().unwrap();

    let r = guard.read_at(&mut buf, off);
    if r.is_err() {
        warn!("Cannot read file {:?}", r);
        return HttpResponse::InternalServerError().finish();
    }

    let rs = r.unwrap();
    info!("Read {:?} bytes", rs);

    HttpResponse::Ok().body(web::Bytes::copy_from_slice(buf.as_slice()))
}


// const std::string url = str::stream()
// << "http://" << _apiUrl << "/os_wt_recovery_write?snapshotId=" << _snapshotId
// << "&filename=" << path << "&offset=" << offset << "&length=" << count;


#[derive(Deserialize)]
struct OsWriteInfo {
    #[serde(rename="snapshotId")]
    snapshot_id:String,
    filename:String,
    offset:i64,
    length:i64,
}

#[post("/os_wt_recovery_write")]
async fn os_wt_recovery_write_44(info: web::Query<OsWriteInfo>, body: Bytes) -> impl Responder {
    // format!("Hello Foo {}!", &info.filename)
    // SEe https://docs.rs/actix-web/3.3.2/actix_web/web/struct.Payload.html

    // let fh = Arc::new(Mutex::new(file.unwrap()));
    let fh_opt =FILE_MAP.get(&info.filename).map(|x| x.clone());
    if fh_opt.is_none() {
        warn!("Cannot find in map: {:?}", info.filename);
        return HttpResponse::InternalServerError()
    }
    let fh = fh_opt.unwrap();

    let off = info.offset as u64;
    let guard = fh.read().unwrap();

    guard.write_at(body.as_ref(), off).unwrap();

    HttpResponse::Ok()
}

// << "http://" << _apiUrl << "/os_list?snapshotId=" << _snapshotId;
#[derive(Deserialize)]
struct OsListInfo {
    #[serde(rename="snapshotId")]
    snapshot_id:String,
}

// one possible implementation of walking a directory only visiting files
fn visit_dirs(dir: &Path, cb: &mut dyn FnMut(&DirEntry)) -> io::Result<()> {
    if dir.is_dir() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                visit_dirs(&path, cb)?;
            } else {
                cb(&entry);
            }
        }
    }
    Ok(())
}

// See /home/mark/src/mongo/src/mongo/db/modules/enterprise/src/queryable/blockstore/list_dir_test.cpp
// Must return BSON ( files : [ {filename:"", fileSize:123},...])
#[get("/os_list")]
async fn os_list(info: web::Query<OsListInfo>) -> impl Responder {
    //format!("Hello Foo {}!", &info.snapshotId);

    let mut doc = Document::new();
    doc.insert("ok".to_string(), true);

    let mut files = Array::new();
    // files.push(Bson::Document(doc!{
    //     "filename": "fake",
    //     "fileSize":100
    // }));
    // files.push(Bson::DateTime(chrono::Utc::now()));
    // files.push(Bson::ObjectId(oid::ObjectId::with_bytes([
    //     1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
    // ])));



    visit_dirs(Path::new(&ROOT_PATH.as_str()), &mut |x : &DirEntry| {

        // 4.0 requires this to be stripped
        // Unsure about 4.4
        let pb = x.path();
        let p = pb.strip_prefix(&ROOT_PATH.as_str()).unwrap();

        // let p2 = Path::new(&ROOT_PREFIX.as_str()).join(p);

        &files.push(Bson::Document(doc!{
            "filename": p.to_str().unwrap(),
            "fileSize": x.metadata().expect("stat filed").len(),
            "blockSize":4096,
        }));
    });


    doc.insert("files".to_string(), Bson::Array(files));

    println!("List files: {:?}", doc);

    let mut buf = Vec::new();
    doc.to_writer(&mut buf).unwrap();

    HttpResponse::Ok().body(web::Bytes::copy_from_slice(buf.as_slice()))

}

// const std::string url = str::stream()
// << "http://" << _apiUrl << "/os_wt_recovery_open_file?snapshotId=" << _snapshotId
// << "&filename=" << path;
#[derive(Deserialize)]
struct OsOpenInfo {
    #[serde(rename="snapshotId")]
    snapshot_id:String,
    filename:String,

}

#[get("/os_wt_recovery_open_file")]
async fn os_wt_recovery_open_file_44(info: web::Query<OsOpenInfo>) -> impl Responder {
    let full_path = Path::new(&ROOT_PATH.as_str()).join(&info.filename);

    // Wiredtiger opens the journal as a "file", just ignore WT
    if full_path.is_dir() {
        warn!("Opening directory {:?}", full_path);
   return HttpResponse::Ok();
    }

    if !full_path.exists() {
        warn!("Creating file {:?}", full_path);
    }


    info!("Opened file: {:?} at '{:?}'", info.filename, full_path);

    let file = OpenOptions::new().read(true).write(true).create(true).open(full_path);
    if file.is_err() {
        warn!("Cannot open file {:?}", file);
        return HttpResponse::InternalServerError();
    }

    let f2 = file.unwrap();
    println!("len: {:?}", f2.metadata().unwrap().len());

    let fh = Arc::new(RwLock::new(f2));
    FILE_MAP.insert(info.filename.to_string(), fh);

    // {
    //     let fh_opt =FILE_MAP.get(&info.filename).map(|x| x.clone());
    //     if fh_opt.is_none() {
    //         warn!("Cannot find in map: {:?}", info.filename);
    //         return HttpResponse::InternalServerError()
    //     }
    //     let fh2 = fh_opt.unwrap();

    // println!("Leng: {:?}", fh2.lock().unwrap().metadata().unwrap().len());

    // }
    // format!("Hello Foo {}!", &info.filename)
    HttpResponse::Ok()
}

// const std::string url = str::stream()
// << "http://" << _apiUrl << "/os_wt_rename_file?snapshotId=" << _snapshotId
// << "&from=" << from << "&to=" << to;
#[derive(Deserialize)]
struct OsRenameInfo {
    #[serde(rename="snapshotId")]
    snapshot_id:String,
    from:String,
    to:String,

}

#[get("/os_wt_rename_file")]
async fn os_wt_rename_file_44(info: web::Query<OsRenameInfo>) -> impl Responder {

    {
        let fh_opt =FILE_MAP.remove(&info.from).map(|x| x.clone());
        if fh_opt.is_none() {
            warn!("Cannot find in map: {:?}", info.from);
            return HttpResponse::InternalServerError()
        }
        let fh = fh_opt.unwrap();
    }

    let from_full = Path::new(&ROOT_PATH.as_str()).join(&info.from);
    let to_full = Path::new(&ROOT_PATH.as_str()).join(&info.to);
    info!("Renaming : {:?} - {:?}", from_full, to_full);
    let r = fs::rename(&from_full, &to_full);
    if r.is_err() {
        warn!("Cannot rename: {:?} - {:?}", from_full, to_full);
        return HttpResponse::InternalServerError()
    }


    let file = OpenOptions::new().read(true).write(true).create(true).open(to_full);
    if file.is_err() {
        warn!("Cannot open file {:?}", file);
        return HttpResponse::InternalServerError();
    }

    let f2 = file.unwrap();
    println!("len: {:?}", f2.metadata().unwrap().len());

    let fh = Arc::new(RwLock::new(f2));
    FILE_MAP.insert(info.to.to_string(), fh);

    HttpResponse::Ok()

}


#[get("/test_close")]
async fn test_close() -> impl Responder {


    HttpResponse::Ok().force_close().finish()

}




/// Search for a pattern in a file and display the lines that contain it.
#[derive(Debug, StructOpt)]
#[structopt(global_settings(&[structopt::clap::AppSettings::ColoredHelp]))]
struct CmdLine {
    #[structopt(flatten)]
    verbose: clap_verbosity_flag::Verbosity,

    #[structopt(name = "debug", short = "d", long = "debug")]
    /// Debug output
    debug: bool,

    #[structopt(name = "use44", long = "use44")]
    /// Use the MongoDB 4.4 version of the protocol
    use44 : bool,
}


#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::from_env(Env::default().default_filter_or("debug")).init();

    let args = CmdLine::from_args();
    let use44 = args.use44;

    HttpServer::new(move || {
        let app = App::new()
            .wrap(Logger::default())
            // .wrap(Logger::new("%a %{User-Agent}i"))
            .route("/", web::get().to(greet))
            // .route("/{name}", web::get().to(greet))
            .service(os_list)
            .service(test_close)
            ;

        if use44 {
            app.service(os_read_44)
            .service(os_wt_recovery_write_44)
            .service(os_wt_recovery_open_file_44)
            .service(os_wt_rename_file_44)
        } else {
            app.service(os_read_40)
        }
    })
    .workers(100)
    .keep_alive(actix_http::KeepAlive::Os)
    //.keep_alive(120)
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}