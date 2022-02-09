# Overview
A tool that works in tandem with its AWS CDK construct (link to be added) that tracks a private package's Code Build project, as well as its consumers so that rebuilds can be coordinated when changes are made.

# What it does currently
This is intended to be run as part of a CodeBuild project's buildspec, and more specifically a project that was created with [insert details of the construct here]. 

# Planned functionality
* Block builds of packages where their consumers are in the process of being built.
  * Would need to find a way to do this without taking up a bunch of idle capacity.
  * Is there a feature in CodeBuild that would help with this?
* Support more than Rust
  * Ideally, there would be a trait defined and implementations for each language that is supported beyond Rust (Java, JavaScript, Python, C#, whatever)