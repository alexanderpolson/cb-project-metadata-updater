mod crate_helper;
mod metadata_updater;

use std::env;
use aws_config::meta::region::RegionProviderChain;
use crate::crate_helper::CrateHelper;
use crate::metadata_updater::{BuildDetails, CrateMetadataUpdater};

const ENV_CODEBUILD_BUILD_ID: &str = "CODEBUILD_BUILD_ID";

#[tokio::main]
async fn main() {
    match update_metadata().await {
        Ok(_) => (),
        Err(err) => eprintln!("ERROR: {}", err.msg)
    }
}

async fn update_metadata() -> Result<(), crate_helper::Error> {
    let build_details = match get_build_details() {
        Ok(build_details) => build_details,
        Err(err) => return Err(err)
    };

    let region_provider = RegionProviderChain::default_provider().or_else("us-west-2");
    let config = aws_config::from_env().region(region_provider).load().await;
    let updater = CrateMetadataUpdater::new(&config);

    updater.update_metadata(build_details, String::from("./Cargo.toml")).await
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