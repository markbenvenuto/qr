use actix_web::{get,post,web, App, HttpRequest, HttpServer, Responder};

use serde::Deserialize;
use actix_web::middleware::Logger;
use env_logger::Env;

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
    snapshotId:String,
    filename:String,
    offset:i64,
    length:i64,
}

#[get("/os_read")]
async fn os_read(info: web::Query<OsReadInfo>) -> impl Responder {
    format!("Hello Foo {}!", &info.filename)
}


// const std::string url = str::stream()
// << "http://" << _apiUrl << "/os_wt_recovery_write?snapshotId=" << _snapshotId
// << "&filename=" << path << "&offset=" << offset << "&length=" << count;


#[derive(Deserialize)]
struct OsWriteInfo {
    snapshotId:String,
    filename:String,
    offset:i64,
    length:i64,
}

#[post("/os_wt_recovery_write")]
async fn os_wt_recovery_write(info: web::Query<OsWriteInfo>, mut body: web::Payload) -> impl Responder {
    format!("Hello Foo {}!", &info.filename)
    // SEe https://docs.rs/actix-web/3.3.2/actix_web/web/struct.Payload.html
}

// << "http://" << _apiUrl << "/os_list?snapshotId=" << _snapshotId;
#[derive(Deserialize)]
struct OsListInfo {
    snapshotId:String,
}

// See /home/mark/src/mongo/src/mongo/db/modules/enterprise/src/queryable/blockstore/list_dir_test.cpp
// Must return BSON ( files : [ {filename:"", fileSize:123},...])
#[get("/os_list")]
async fn os_list(info: web::Query<OsListInfo>) -> impl Responder {
    format!("Hello Foo {}!", &info.snapshotId)
}

// const std::string url = str::stream()
// << "http://" << _apiUrl << "/os_wt_recovery_open_file?snapshotId=" << _snapshotId
// << "&filename=" << path;
#[derive(Deserialize)]
struct OsOpenInfo {
    snapshotId:String,
    filename:String,

}

#[get("/os_wt_recovery_open_file")]
async fn os_wt_recovery_open_file(info: web::Query<OsOpenInfo>) -> impl Responder {
    format!("Hello Foo {}!", &info.filename)
}

// const std::string url = str::stream()
// << "http://" << _apiUrl << "/os_wt_rename_file?snapshotId=" << _snapshotId
// << "&from=" << from << "&to=" << to;
#[derive(Deserialize)]
struct OsRenameInfo {
    snapshotId:String,
    from:String,
    to:String,

}

#[get("/os_wt_rename_file")]
async fn os_wt_rename_file(info: web::Query<OsRenameInfo>) -> impl Responder {
    format!("Hello Foo {}!", &info.from)
}


#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::from_env(Env::default().default_filter_or("debug")).init();


    HttpServer::new(|| {
        App::new()
            .wrap(Logger::default())
            .wrap(Logger::new("%a %{User-Agent}i"))
            .route("/", web::get().to(greet))
            // .route("/{name}", web::get().to(greet))
            .service(os_read)
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}