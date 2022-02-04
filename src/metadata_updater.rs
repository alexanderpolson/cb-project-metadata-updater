use aws_config::Config;
use aws_sdk_dynamodb::{Client as DynamoDbClient, SdkError};
use aws_sdk_dynamodb::model::{AttributeValue, AttributeValueUpdate};
use futures::future::{BoxFuture, try_join_all};
use crate::{CrateHelper, crate_helper};
use crate::crate_helper::Dependency;

const LANGUAGE: &str = "rust";
const DYNAMO_DB_TABLE: &str = "PackageMetadata";

pub struct BuildDetails {
    pub build_project_name: String
}

pub struct CrateMetadataUpdater {
    ddb: DynamoDbClient
}

impl CrateMetadataUpdater {
    pub fn new(client_config: &Config) -> CrateMetadataUpdater {
        CrateMetadataUpdater {
            ddb: DynamoDbClient::new(client_config)
        }
    }

    pub async fn update_metadata(self, build_details: BuildDetails, path: String) -> Result<(), crate_helper::Error> {
        let crt = match CrateHelper::from_path(path) {
            Ok(crt) => crt,
            Err(err) => return Err(err)
        };

        let name = crt.name();
        let version = crt.version();
        let primary_key = get_primary_key(&name, &version);
        
        // https://docs.rs/futures/latest/futures/future/fn.try_join_all.html
        // https://users.rust-lang.org/t/how-to-execute-multiple-async-fns-at-once-and-use-join-all-to-get-all-their-results/47437/4
        let mut step_futures: Vec<BoxFuture<_>> = vec![
            Box::pin(self.update_project_name(&primary_key, &build_details)),
            // TODO: Kick off builds for consuming and tracked packages.
        ];
        for dep in crt.dependencies {
            step_futures.push(Box::pin(self.update_dependency(&primary_key, dep)));
        }
        match try_join_all(step_futures).await {
            Ok(_) => Ok(()),
            Err(err) => Err(err)
        }
    }

    async fn update_project_name(&self, primary_key: &String, build_details: &BuildDetails) -> Result<(), crate_helper::Error> {
        match self.ddb.update_item()
            .table_name(String::from(DYNAMO_DB_TABLE))
            .key("package_name", AttributeValue::S(primary_key.clone()))
            .attribute_updates("code_build_project_name",
                               AttributeValueUpdate::builder()
                                   .value(AttributeValue::S(build_details.build_project_name.clone()))
                                   .build())
            .send().await {
            Ok(_) => {
                Ok(())
            },
            Err(err) => {
                return Err(crate_helper::Error {
                    msg: err.to_string()
                })
            }
        }
    }

    async fn update_dependency(&self, primary_key: &String, dep: Dependency) -> Result<(), crate_helper::Error> {
        match &dep.version {
            Some(version) => {
                // If a record for this dependency exists, then add the current crate as a consumer
                // of it.
                let fq_dep_name = get_primary_key(&dep.name, &version);
                match self.ddb.update_item()
                    .table_name(String::from(DYNAMO_DB_TABLE))
                    .key("package_name", AttributeValue::S(fq_dep_name.clone()))
                    .update_expression("ADD consumers :d")
                    .expression_attribute_values(":d", AttributeValue::Ss(vec![primary_key.clone()]))
                    .condition_expression("attribute_exists(package_name)").send().await {
                    Ok(_) => log::info!("{} added.", fq_dep_name),
                    Err(err) => {
                        match err {
                            SdkError::ServiceError {err, ..} => {
                                if err.is_conditional_check_failed_exception() {
                                    eprintln!("{} not being tracked. Skipping...", fq_dep_name)
                                } else {
                                    return Err(crate_helper::Error::with_msg(format!("ERROR: {}", err.to_string())))
                                }
                            },
                            _ => return Err(crate_helper::Error::with_msg(format!("ERROR: {}", err.to_string())))
                        }
                    }
                }
            }
            None => {
                log::error!("Crate {} doesn't have a version specified.", dep.name)
            }
        }
        Ok(())
    }
}

fn get_primary_key(crate_name: &String, version: &String) -> String {
    format!("{}/{}/{}", LANGUAGE, crate_name, version)
}