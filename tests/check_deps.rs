use std::{
    io::Write,
    process::{Command, Stdio},
};

#[test]
fn no_circular_dependencies() {
    let dot = Command::new("cargo")
        .args(["modules", "dependencies", "--no-fns"])
        .output()
        .expect("failed to run `cargo modules dependencies`");

    assert!(
        dot.status.success(),
        "cargo modules failed:\n{}",
        String::from_utf8_lossy(&dot.stderr),
    );

    let mut child = Command::new("tred")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to run `tred` (is graphviz installed?)");

    child
        .stdin
        .take()
        .unwrap()
        .write_all(&dot.stdout)
        .unwrap();

    let output = child.wait_with_output().unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        !stderr.contains("cycle"),
        "circular dependency detected:\n{stderr}",
    );
}
