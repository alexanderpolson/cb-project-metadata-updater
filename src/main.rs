mod crate_helper;
mod metadata_updater;

use std::env;
use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_config::meta::region::RegionProviderChain;
use crate::crate_helper::CrateHelper;
use crate::metadata_updater::{BuildDetails, CrateMetadataUpdater};

const ENV_CODEBUILD_BUILD_ID: &str = "CODEBUILD_BUILD_ID";
const ENV_PKG_METADATA_TABLE: &str = "PKG_METADATA_TABLE";

#[tokio::main]
async fn main() {
    // TODO: Add command-line arguments
    // https://doc.rust-lang.org/book/ch12-01-accepting-command-line-arguments.html
    match update_metadata().await {
        Ok(_) => (),
        Err(err) => {
            eprintln!("ERROR: {}", err.msg);
            std::process::exit(1)
        }
    }
}

async fn update_metadata() -> Result<(), crate_helper::Error> {
    let build_details = match get_build_details() {
        Ok(build_details) => build_details,
        Err(err) => return Err(err)
    };

    // This is a hack for quick support for local profiles. Arguments should be properly fleshed out
    let mut credential_chain =
        DefaultCredentialsChain::builder()
            .region(RegionProviderChain::default_provider().or_else("us-west-2"));
    let args: Vec<String> = env::args().collect();
    if let Some(profile_name) = args.get(1) {
        eprintln!("Using AWS profile \"{}\"", profile_name);
        credential_chain = credential_chain.profile_name(profile_name);
    }
    let config =
        aws_config::from_env()
            .credentials_provider(credential_chain.build().await).load().await;
    match std::env::var(ENV_PKG_METADATA_TABLE) {
        Ok(table_value) => {
            eprintln!("Writing changes to {} table.", table_value);
            let updater = CrateMetadataUpdater::new(&config, table_value);
            updater.update_metadata(build_details, String::from("./Cargo.toml")).await
        },
        Err(_) => Err(crate_helper::Error::with_msg(format!("Unable to determine Package Metadata table name from {} env variable", ENV_PKG_METADATA_TABLE)))
    }
}

fn get_build_details() -> Result<BuildDetails, crate_helper::Error> {
    match env::var(ENV_CODEBUILD_BUILD_ID) {
        Ok(build_id) => {
            let parts: Vec<&str> = build_id.split(":").collect();
            let build_project_name = String::from(
                *parts.get(0)
                    .expect("Expected string of pattern \"ProjectName:UUID\"")
            );
            Ok(BuildDetails {
                build_project_name
            })
        },
        Err(_) => Err(crate_helper::Error {
            msg: format!("Didn't find {} env var", ENV_CODEBUILD_BUILD_ID)
        })
    }
}