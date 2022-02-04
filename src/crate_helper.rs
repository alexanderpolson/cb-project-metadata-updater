use std::path::Path;
use cargo_toml::{Manifest, Package};
use cargo_toml::Dependency::{Detailed, Simple};

pub struct Dependency {
    pub name: String,
    pub version: Option<String>,
}

pub struct CrateHelper {
    package: Package,
    pub dependencies: Vec<Dependency>,
}

#[derive(Debug)]
pub struct Error {
    pub msg: String,
}

impl Error {
    pub fn with_msg(msg: String) -> Error {
        Error {
            msg
        }
    }
}

impl CrateHelper {
    pub fn from_path(cargo_toml_path: impl AsRef<Path>) -> Result<Self, Error> {
        match Manifest::from_path(cargo_toml_path) {
            Ok(manifest) => {
                match manifest.package {
                    Some(package) => {
                        // Gather dependencies
                        let mut dependencies: Vec<Dependency> = Vec::new();
                        for (name, dep) in manifest.dependencies {
                            let version = match dep {
                                Simple(version) => Some(version),
                                Detailed(details) => details.version
                            };
                            dependencies.push(Dependency { name, version });
                        }
                        Ok(CrateHelper {
                            package,
                            dependencies,
                        })
                    },
                    None => Err(Error {
                        msg: format!("No package section present in Cargo.toml")
                    } )
                }
            },
            Err(_) => Err(Error {
                msg: format!("Can't find Cargo.toml in current path")
            })
        }
    }

    pub fn name(&self) -> String {
        self.package.name.clone()
    }

    pub fn version(&self) -> String {
        self.package.version.clone()
    }
}