use std::{fs, io::Write, path::Path};

use error_code::*;

fn main() {
    let err_codes = vec![
        codec::ALL_ERROR_CODES.iter(),
        interlock::ALL_ERROR_CODES.iter(),
        encryption::ALL_ERROR_CODES.iter(),
        engine::ALL_ERROR_CODES.iter(),
        fidel::ALL_ERROR_CODES.iter(),
        violetabft::ALL_ERROR_CODES.iter(),
        violetabftstore::ALL_ERROR_CODES.iter(),
        sst_importer::ALL_ERROR_CODES.iter(),
        causet_storage::ALL_ERROR_CODES.iter(),
    ];
    let path = Path::new("./etc/error_code.toml");
    let mut f = fs::File::create(&path).unwrap();
    err_codes
        .into_iter()
        .flatten()
        .map(|c| {
            let s = toml::to_string_pretty(c).unwrap();
            format!("[error.{}]\n{}\n", c.code, s.as_str())
        })
        .for_each(|s| {
            f.write_all(s.as_bytes()).unwrap();
        });

    f.sync_all().unwrap();
}
