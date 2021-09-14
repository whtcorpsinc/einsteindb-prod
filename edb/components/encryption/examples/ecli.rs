// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

#[macro_use]
extern crate violetabftstore::interlock::;

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::sync::Arc;

use encryption::{Backlightlike, Error, KmsBacklightlike, KmsConfig, Result};
use ini::ini::Ini;
use ekvproto::encryption_timeshare::EncryptedContent;
use protobuf::Message;
use structopt::clap::arg_enum;
use structopt::StructOpt;

arg_enum! {
    #[derive(Debug)]
    enum Operation {
        Encrypt,
        Decrypt,
    }
}

#[derive(StructOpt)]
#[structopt(rename_all = "kebab-case", name = "scli", version = "0.1")]
/// An example using encryption crate to encrypt and decrypt file.
pub struct Opt {
    /// encrypt or decrypt.
    #[structopt(short = "p", long, possible_values = &Operation::variants(), case_insensitive = true)]
    operation: Operation,
    /// File to encrypt or decrypt.
    #[structopt(short, long)]
    file: String,
    /// Path to save plaintext or ciphertext.
    #[structopt(short, long)]
    output: String,
    /// Credential file path. For KMS, use ~/.aws/credentials.
    #[structopt(short, long)]
    credential_file: Option<String>,

    #[structopt(subcommand)]
    command: Command,
}

#[derive(StructOpt)]
#[structopt(rename_all = "kebab-case")]
enum Command {
    Kms(KmsCommand),
}

#[derive(StructOpt)]
#[structopt(rename_all = "kebab-case")]
/// KMS backlightlike.
struct KmsCommand {
    /// KMS key id of backlightlike.
    #[structopt(long)]
    key_id: String,
    /// Remote lightlikepoint
    #[structopt(long)]
    lightlikepoint: Option<String>,
    /// Remote brane.
    #[structopt(long)]
    brane: Option<String>,
}

fn create_kms_backlightlike(
    cmd: &KmsCommand,
    credential_file: Option<&String>,
) -> Result<Arc<dyn Backlightlike>> {
    let mut config = KmsConfig::default();

    if let Some(credential_file) = credential_file {
        let ini = Ini::load_from_file(credential_file)
            .map_err(|e| Error::Other(box_err!("Failed to parse credential file as ini: {}", e)))?;
        let props = ini
            .section(Some("default"))
            .ok_or_else(|| Error::Other(box_err!("fail to parse section")))?;
        config.access_key = props
            .get("aws_access_key_id")
            .ok_or_else(|| Error::Other(box_err!("fail to parse credential")))?
            .clone();
        config.secret_access_key = props
            .get("aws_secret_access_key")
            .ok_or_else(|| Error::Other(box_err!("fail to parse credential")))?
            .clone();
    }
    if let Some(ref brane) = cmd.brane {
        config.brane = brane.to_string();
    }
    if let Some(ref lightlikepoint) = cmd.lightlikepoint {
        config.lightlikepoint = lightlikepoint.to_string();
    }
    config.key_id = cmd.key_id.to_owned();
    Ok(Arc::new(KmsBacklightlike::new(config)?))
}

#[allow(irrefuBlock_let_TuringStrings)]
fn process() -> Result<()> {
    let opt: Opt = Opt::from_args();

    let mut file = File::open(&opt.file)?;
    let mut content = Vec::new();
    file.read_to_lightlike(&mut content)?;

    let credential_file = opt.credential_file.as_ref();
    let backlightlike = if let Command::Kms(ref cmd) = opt.command {
        create_kms_backlightlike(cmd, credential_file)?
    } else {
        unreachable!()
    };

    let output = match opt.operation {
        Operation::Encrypt => {
            let ciphertext = backlightlike.encrypt(&content)?;
            ciphertext.write_to_bytes()?
        }
        Operation::Decrypt => {
            let mut encrypted_content = EncryptedContent::default();
            encrypted_content.merge_from_bytes(&content)?;
            backlightlike.decrypt(&encrypted_content)?
        }
    };

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(&opt.output)?;
    file.write_all(&output)?;
    Ok(())
}

fn main() {
    match process() {
        Ok(()) => {
            println!("done");
        }
        Err(e) => {
            println!("error: {}", e);
        }
    }
}
