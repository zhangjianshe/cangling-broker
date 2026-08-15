use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);
    tonic_build::compile_protos("proto/queue.proto")?;

    println!("cargo:rerun-if-env-changed=GIT_HASH");
    println!("cargo:rerun-if-env-changed=BUILD_TIME");
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/refs/heads");

    println!("cargo:rustc-env=GIT_HASH={}", git_hash());
    println!("cargo:rustc-env=BUILD_TIME={}", build_time());
    Ok(())
}

fn git_hash() -> String {
    if let Some(value) = env_override("GIT_HASH") {
        return value;
    }
    command_stdout(["git", "rev-parse", "--short", "HEAD"]).unwrap_or_else(|| "unknown".into())
}

fn build_time() -> String {
    if let Some(value) = env_override("BUILD_TIME") {
        return value;
    }
    command_stdout(["date", "-u", "+%Y-%m-%dT%H:%M:%SZ"]).unwrap_or_else(|| "unknown".into())
}

fn env_override(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn command_stdout<const N: usize>(args: [&str; N]) -> Option<String> {
    let (program, rest) = args.split_first()?;
    let output = Command::new(program).args(rest).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    let text = text.trim();
    if text.is_empty() {
        None
    } else {
        Some(text.to_string())
    }
}
