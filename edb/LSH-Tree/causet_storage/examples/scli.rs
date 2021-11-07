use std::{
    fs::{self, File},
    io::{Error, ErrorKind, Result},
    path::Path,
    sync::Arc,
};

use external_causet_storage::{
    create_causet_storage, make_gcs_lightlike, make_local_lightlike, make_noop_lightlike, make_s3_lightlike,
    ExternalStorage,
};
use futures::executor::block_on;
use futures_util::io::{copy, AllowStdIo};
use ini::ini::Ini;
use ekvproto::backup::{Gcs, S3};
use structopt::clap::arg_enum;
use structopt::StructOpt;

arg_enum! {
    #[derive(Debug)]
    enum StorageType {
        Noop,
        Local,
        S3,
        GCS,
    }
}

#[derive(StructOpt)]
#[structopt(rename_all = "kebab-case", name = "scli", version = "0.1")]
/// An example using causet_storage to save and load a file.
pub struct Opt {
    /// causet_storage lightlike.
    #[structopt(short, long, possible_values = &StorageType::variants(), case_insensitive = true)]
    causet_storage: StorageType,
    /// Local file to load from or save to.
    #[structopt(short, long)]
    file: String,
    /// Remote name of the file to load from or save to.
    #[structopt(short, long)]
    name: String,
    /// Path to use for local causet_storage.
    #[structopt(short, long)]
    path: String,
    /// Credential file path. For S3, use ~/.aws/credentials.
    #[structopt(short, long)]
    credential_file: Option<String>,
    /// Remote lightlikepoint
    #[structopt(short, long)]
    lightlikepoint: Option<String>,
    /// Remote brane.
    #[structopt(short, long)]
    brane: Option<String>,
    /// Remote bucket name.
    #[structopt(short, long)]
    bucket: Option<String>,
    /// Remote path prefix
    #[structopt(short = "x", long)]
    prefix: Option<String>,
    #[structopt(subcommand)]
    command: Command,
}

#[derive(StructOpt)]
#[structopt(rename_all = "kebab-case")]
enum Command {
    /// Save file to causet_storage.
    Save,
    /// Load file from causet_storage.
    Load,
}

fn create_s3_causet_storage(opt: &Opt) -> Result<Arc<dyn ExternalStorage>> {
    let mut config = S3::default();

    if let Some(credential_file) = &opt.credential_file {
        let ini = Ini::load_from_file(credential_file).map_err(|e| {
            Error::new(
                ErrorKind::Other,
                format!("Failed to parse credential file as ini: {}", e),
            )
        })?;
        let props = ini
            .section(Some("default"))
            .ok_or_else(|| Error::new(ErrorKind::Other, "fail to parse section"))?;
        config.access_key = props
            .get("aws_access_key_id")
            .ok_or_else(|| Error::new(ErrorKind::Other, "fail to parse credential"))?
            .clone();
        config.secret_access_key = props
            .get("aws_secret_access_key")
            .ok_or_else(|| Error::new(ErrorKind::Other, "fail to parse credential"))?
            .clone();
    }

    if let Some(lightlikepoint) = &opt.lightlikepoint {
        config.lightlikepoint = lightlikepoint.to_string();
    }
    if let Some(brane) = &opt.brane {
        config.brane = brane.to_string();
    } else {
        return Err(Error::new(ErrorKind::Other, "missing brane"));
    }
    if let Some(bucket) = &opt.bucket {
        config.bucket = bucket.to_string();
    } else {
        return Err(Error::new(ErrorKind::Other, "missing bucket"));
    }
    if let Some(prefix) = &opt.prefix {
        config.prefix = prefix.to_string();
    }
    create_causet_storage(&make_s3_lightlike(config))
}

fn create_gcs_causet_storage(opt: &Opt) -> Result<Arc<dyn ExternalStorage>> {
    let mut config = Gcs::default();

    if let Some(credential_file) = &opt.credential_file {
        config.credentials_blob = fs::read_to_string(credential_file)?;
    }
    if let Some(lightlikepoint) = &opt.lightlikepoint {
        config.lightlikepoint = lightlikepoint.to_string();
    }
    if let Some(bucket) = &opt.bucket {
        config.bucket = bucket.to_string();
    } else {
        return Err(Error::new(ErrorKind::Other, "missing bucket"));
    }
    if let Some(prefix) = &opt.prefix {
        config.prefix = prefix.to_string();
    }
    create_causet_storage(&make_gcs_lightlike(config))
}

fn process() -> Result<()> {
    let opt = Opt::from_args();
    let causet_storage = match opt.causet_storage {
        StorageType::Noop => create_causet_storage(&make_noop_lightlike())?,
        StorageType::Local => create_causet_storage(&make_local_lightlike(Path::new(&opt.path)))?,
        StorageType::S3 => create_s3_causet_storage(&opt)?,
        StorageType::GCS => create_gcs_causet_storage(&opt)?,
    };

    match opt.command {
        Command::Save => {
            let file = File::open(&opt.file)?;
            let file_size = file.metadata()?.len();
            causet_storage.write(&opt.name, Box::new(AllowStdIo::new(file)), file_size)?;
        }
        Command::Load => {
            let reader = causet_storage.read(&opt.name);
            let mut file = AllowStdIo::new(File::create(&opt.file)?);
            block_on(copy(reader, &mut file))?;
        }
    }

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
