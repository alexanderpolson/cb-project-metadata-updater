use std::collections::HashSet;
use aws_config::Config;
use aws_sdk_codebuild::{Client as CodeBuildClient};
use aws_sdk_dynamodb::{Client as DynamoDbClient, SdkError as DynamoDbError};
use aws_sdk_dynamodb::model::{AttributeAction, AttributeValue, AttributeValueUpdate, ReturnValue};
use futures::future::try_join_all;
use crate::{CrateHelper, crate_helper};
use crate::crate_helper::Dependency;

const LANGUAGE: &str = "rust";
const DYNAMO_DB_TABLE: &str = "PackageMetadata";

const KEY_CODE_BUILD_PROJECT_NAME: &str = "code_build_project_name";
const KEY_PACKAGE_NAME: &str = "package_name";
const KEY_CONSUMERS: &str = "consumers";
const KEY_DEPENDENCIES: &str = "dependencies";

pub struct BuildDetails {
    pub build_project_name: String
}

pub struct CrateMetadataUpdater {
    ddb: DynamoDbClient,
    codebuild: CodeBuildClient,
    pkg_metadata_table: String,
}

impl CrateMetadataUpdater {
    pub fn new(client_config: &Config, pkg_metadata_table: String) -> CrateMetadataUpdater {
        CrateMetadataUpdater {
            ddb: DynamoDbClient::new(client_config),
            codebuild: CodeBuildClient::new(client_config),
            pkg_metadata_table,
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
        // First update those dependencies that are being tracked with this crate as a consumer.
        // This allows us to then store only those dependencies that are tracked.
        for dep in &crt.dependencies {
            dep_update_futures.push(Box::pin(self.add_consumer_to_dependency(&primary_key, dep)));
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
                tracked_deps
            },
            Err(err) => return Err(err)
        };

        match self.update_project(&primary_key, &build_details, tracked_deps).await {
            Ok(_) => Ok(()),
            Err(err) => return Err(err)
        }
    }

    async fn add_consumer_to_dependency(&self, primary_key: &String, dep: &Dependency) -> Result<Option<String>, crate_helper::Error> {
        // TODO: Need to update all dependencies that match the version pattern.
        // When adding a consumer, it needs to be added to all matching versions.
        // NOTE: This probably isn't entirely true, but makes things a bit easier.
        // Crate: https://docs.rs/semver/latest/semver/index.html
        // https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html
        match &dep.version {
            Some(version) => {
                // If a record for this dependency exists, then add the current crate as a consumer
                // of it.
                let fq_dep_name = get_primary_key(&dep.name, &version);
                match self.ddb.update_item()
                    .table_name(String::from(DYNAMO_DB_TABLE))
                    .key(KEY_PACKAGE_NAME, AttributeValue::S(fq_dep_name.clone()))
                    .update_expression(format!("ADD {} :d", KEY_CONSUMERS))
                    .expression_attribute_values(":d", AttributeValue::Ss(vec![primary_key.clone()]))
                    .condition_expression(format!("attribute_exists({})", KEY_PACKAGE_NAME)).send().await {
                    Ok(_) => {
                        log::info!("{} added as consumer of {}.", primary_key, fq_dep_name);
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

    async fn update_project(&self, primary_key: &String, build_details: &BuildDetails, tracked_deps: Vec<String>) -> Result<(), crate_helper::Error> {
        // TODO: Always update the specific version.
        // We won't (and shouldn't) try and rebuild all projects that would consume a new version as
        // the actual versions being used by the consumer should be locked, until it's rebuilt, at
        // which point, it will grab the appropriate version and add itself as a consumer to that
        // version.
        // Also, the single CodeBuild project per codebase doesn't work if multiple versions of the
        // package are active. For example, v1 and v2 and applying patches to both versions.
        let tracked_deps_set = to_set(&tracked_deps);
        let dep_attribute_update = if tracked_deps.is_empty() {
            // If tracked_deps is empty, delete the dependencies value.
            AttributeValueUpdate::builder()
                .action(AttributeAction::Delete)
        } else {
            AttributeValueUpdate::builder()
                .value(AttributeValue::Ss(tracked_deps))
        }.build();

        match self.ddb.update_item()
            .table_name(String::from(DYNAMO_DB_TABLE))
            .key(KEY_PACKAGE_NAME, AttributeValue::S(primary_key.clone()))
            .attribute_updates(KEY_CODE_BUILD_PROJECT_NAME,
                               AttributeValueUpdate::builder()
                                   .value(AttributeValue::S(build_details.build_project_name.clone()))
                                   .build())
            .attribute_updates(KEY_DEPENDENCIES, dep_attribute_update)
            .return_values(ReturnValue::AllOld)
            .send().await {
            Ok(response) => {
                if let Some(old_attributes) = response.attributes {
                    if let Some(old_deps_av) = old_attributes.get(KEY_DEPENDENCIES) {
                        if let Ok(old_deps) = old_deps_av.as_ss() {
                            let old_deps_set = to_set(old_deps);
                            // Clean up old dependencies that should no longer exist.
                            let mut dep_rm_futures = vec![];
                            for old_dep in old_deps_set.difference(&tracked_deps_set) {
                                dep_rm_futures.push(Box::pin(self.rm_consumer_from_dependency(primary_key, old_dep)));
                            }
                            if !dep_rm_futures.is_empty() {
                                match try_join_all(dep_rm_futures).await {
                                    Ok(_) => (),
                                    Err(err) => return Err(err)
                                }
                            }
                        }
                    }
                    if let Some(consumers_av) = old_attributes.get(KEY_CONSUMERS) {
                        if let Ok(consumers) = consumers_av.as_ss() {
                            println!("Consumers: {:?}", consumers_av);
                            let mut project_build_futures = vec![];
                            for consumer_key in consumers {
                                project_build_futures.push(Box::pin(self.rebuild_consumer(primary_key, consumer_key)));
                            }
                            match try_join_all(project_build_futures).await {
                                Ok(_) => (),
                                Err(err) => return Err(err),
                            }
                        }
                    }
                }
                Ok(())
            },
            Err(err) => {
                return Err(crate_helper::Error {
                    msg: err.to_string()
                })
            }
        }
    }

    async fn rm_consumer_from_dependency(&self, primary_key: &String, fq_dep_name: &String) -> Result<(), crate_helper::Error> {
        eprintln!("Trying to remove {} as consumer of {}.", primary_key, fq_dep_name);
        match self.ddb.update_item()
            .table_name(String::from(DYNAMO_DB_TABLE))
            .key(KEY_PACKAGE_NAME, AttributeValue::S(fq_dep_name.clone()))
            .update_expression(format!("DELETE {} :d", KEY_CONSUMERS))
            .expression_attribute_values(":d", AttributeValue::Ss(vec![primary_key.clone()]))
            .condition_expression(format!("attribute_exists({})", KEY_PACKAGE_NAME)).send().await {
            Ok(_) => {
                log::info!("{} removed as consumer of {}.", primary_key, fq_dep_name);
                Ok(())
            },
            Err(err) => {
                match err {
                    DynamoDbError::ServiceError { err, .. } => {
                        if err.is_conditional_check_failed_exception() {
                            eprintln!("{} not being tracked. Skipping...", fq_dep_name);
                            Ok(())
                        } else {
                            return Err(crate_helper::Error::with_msg(format!("ERROR: {}", err.to_string())))
                        }
                    },
                    _ => return Err(crate_helper::Error::with_msg(format!("ERROR: {}", err.to_string())))
                }
            }
        }
    }

    async fn rebuild_consumer(&self, primary_key: &String, consumer_key: &String) -> Result<(), crate_helper::Error> {
        eprintln!("Checking to see if {} needs to be rebuilt due to update to {}.", consumer_key, primary_key);
        match self.ddb.get_item()
            .table_name(String::from(DYNAMO_DB_TABLE))
            .key(KEY_PACKAGE_NAME, AttributeValue::S(consumer_key.clone()))
            .send().await {
            Ok(response) => {
                eprintln!("Found record for {}: {:?}", consumer_key, response);
                if let Some(item) = response.item {
                    if let Some(dependencies_av) = item.get(KEY_DEPENDENCIES) {
                        if let Ok(dependencies) = dependencies_av.as_ss() {
                            eprintln!("Found the following dependencies for {}: {:?}", primary_key, dependencies);
                            if dependencies.contains(primary_key) {
                                if let Some(cb_build_project_av) = item.get(KEY_CODE_BUILD_PROJECT_NAME) {
                                    if let Ok(cb_build_project_name) = cb_build_project_av.as_s() {
                                        return match self.codebuild.start_build().project_name(cb_build_project_name).send().await {
                                            Ok(_) => {
                                                eprintln!("Kicked off rebuild of consumer {} (CB project {}", consumer_key, cb_build_project_name);
                                                Ok(())
                                            },
                                            Err(err) => return Err(crate_helper::Error::with_msg(format!("ERROR: {}", err.to_string())))
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(())
            },
            Err(err) => Err(crate_helper::Error::with_msg(format!("ERROR: {}", err.to_string())))
        }
    }
}

fn to_set(vec: &Vec<String>) -> HashSet<String> {
    let mut set: HashSet<String> = HashSet::new();
    for element in vec {
        set.insert(element.clone());
    }
    set
}

fn get_primary_key(crate_name: &String, version: &String) -> String {
    format!("{}/{}/{}", LANGUAGE, crate_name, version)
}