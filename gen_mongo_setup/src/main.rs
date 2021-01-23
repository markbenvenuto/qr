use std::io::Cursor;
use bson::{oid, Array, Bson, Document};
use bson::doc;

use std::fs::File;
use std::io::{Write, BufReader, BufRead, Error};


fn main() {
    let doc = doc! {
        // "hello": "world",
        // "int": 5,
        // "subdoc": { "cat": true },
        "storage":{"engine":"queryable_wt"}
     };

     let mut buf = Vec::new();
     doc.to_writer(&mut buf).unwrap();

     println!("Serialized: {:?}", buf);


     let mut output = File::create("storage.bson").unwrap();
     output.write(&buf);
}
