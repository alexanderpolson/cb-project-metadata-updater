use aws_config::Config;
use aws_sdk_codebuild::{Client as CodeBuildClient, SdkError as CodeBuildError};
use aws_sdk_dynamodb::{Client as DynamoDbClient, SdkError as DynamoDbError};
use aws_sdk_dynamodb::model::{AttributeValue, AttributeValueUpdate, ReturnValue};
use futures::future::{BoxFuture, try_join_all};
use crate::{CrateHelper, crate_helper};
use crate::crate_helper::Dependency;

const LANGUAGE: &str = "rust";
const DYNAMO_DB_TABLE: &str = "PackageMetadata";

pub struct BuildDetails {
    pub build_project_name: String
}

pub struct CrateMetadataUpdater {
    ddb: DynamoDbClient,
    codebuild: CodeBuildClient,
}

impl CrateMetadataUpdater {
    pub fn new(client_config: &Config) -> CrateMetadataUpdater {
        CrateMetadataUpdater {
            ddb: DynamoDbClient::new(client_config),
            codebuild: CodeBuildClient::new(client_config),
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
        let mut dep_update_futures = vec![];
        // TODO: Track list of dependencies, so that if they change, the associated consumer entry for the dependency can be removed.
        // First update those dependencies that are being tracked with this crate as a consumer.
        // This allows us to then store only those dependencies that are tracked.
        for dep in &crt.dependencies {
            dep_update_futures.push(Box::pin(self.update_dependency(&primary_key, dep)));
        }
        // TODO: Get list of consumers of this package.
        // TODO: Kick off builds for each of the consumer projects.
        let tracked_deps = match try_join_all(dep_update_futures).await {
            Ok(deps) => {
                let mut tracked_deps: Vec<String> = vec![];
                for dep in deps {
                    if let Some(tracked_dep) = dep {
                        tracked_deps.push(tracked_dep);
                    }
                }
            },
            Err(err) => return Err(err)
        };

        self.update_project(&primary_key, &build_details, &crt);
        Ok(())
    }

    async fn update_project(&self, primary_key: &String, build_details: &BuildDetails, crt: &CrateHelper) -> Result<(), crate_helper::Error> {
        match self.ddb.update_item()
            .table_name(String::from(DYNAMO_DB_TABLE))
            .key("package_name", AttributeValue::S(primary_key.clone()))
            .attribute_updates("code_build_project_name",
                               AttributeValueUpdate::builder()
                                   .value(AttributeValue::S(build_details.build_project_name.clone()))
                                   .build())
            .attribute_updates("dependencies",
                               AttributeValueUpdate::builder()
                                   .value(AttributeValue::Ss(dependency_primary_keys(&crt)))
                                   .build())
            .send().await {
            Ok(response) => {
                eprintln!("{:?}", response);
                Ok(())
            },
            Err(err) => {
                return Err(crate_helper::Error {
                    msg: err.to_string()
                })
            }
        }
    }

    async fn update_dependency(&self, primary_key: &String, dep: &Dependency) -> Result<Option<String>, crate_helper::Error> {
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
                    Ok(_) => {
                        log::info!("{} added.", fq_dep_name);
                        Ok(Some(fq_dep_name))
                    },
                    Err(err) => {
                        match err {
                            DynamoDbError::ServiceError {err, ..} => {
                                if err.is_conditional_check_failed_exception() {
                                    eprintln!("{} not being tracked. Skipping...", fq_dep_name);
                                    Ok(None)
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
                log::error!("Crate {} doesn't have a version specified.", dep.name);
                Ok(None)
            }
        }
    }
}

fn get_primary_key(crate_name: &String, version: &String) -> String {
    format!("{}/{}/{}", LANGUAGE, crate_name, version)
}

fn dependency_primary_keys(crt: &CrateHelper) -> Vec<String> {
    let mut primary_keys: Vec<String> = vec![];
    for dep in &crt.dependencies {
        if let Some(version) = &dep.version {
            primary_keys.push(get_primary_key(&dep.name, &version));
        }
    }
    primary_keys
}