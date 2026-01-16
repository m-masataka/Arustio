fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                "proto/mount.proto",
                "proto/file.proto",
                "proto/meta.proto",
                "proto/snapshot.proto",
                "proto/raftio.proto",
            ],
            &["proto"],
        )?;
    Ok(())
}
