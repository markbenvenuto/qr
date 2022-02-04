qr is a passthrough emulation of queryable backup

# How to use

Install rust (see https://rustup.rs/) and cargo

The tool assumes the following:
1. Your MongoDB files are in `/data/db`
2. The mock webserver runs on `http://localhost:8080`

# Setup MongoDB
1. Start up MongoDB normally
2. Add some data to your database
3. Shutdown


# Setup Queryable
1. Make a directory, "qr" in this example
2. Create "storage.bson" file in this directory that tells MongoDB to use queryable backup
   1. cd gen_mongo_setup
   2. cargo run
   3. It prints the contents of "storage.bson" to screen and saves it to a file
   4. Copy storage.bson to your "qr" directory
3. rm mongod.lock;
4. Start the fake web server
   1. To run the fake web server tool, `cargo run --release`
   2. Release is faster then the default of debug
5. Run MongoDB
`SECRET_KEY=fake ./mongod --queryableBackupApiUri=localhost:8080 --queryableSnapshotId=123456781234567812345678 --dbpath=qr --storageEngine=queryable_wt --queryableBackupMode --wiredTigerCacheSizeGB=1 --slowms=10000`
6. MongoDB should start up
