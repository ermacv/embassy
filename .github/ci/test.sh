#!/bin/bash
## on push branch~=gh-readonly-queue/main/.*
## on pull_request

set -euo pipefail

export RUSTUP_HOME=/ci/cache/rustup
export CARGO_HOME=/ci/cache/cargo
export CARGO_TARGET_DIR=/ci/cache/target

# needed for "dumb HTTP" transport support
# used when pointing stm32-metapac to a CI-built one.
export CARGO_NET_GIT_FETCH_WITH_CLI=true

cargo test --manifest-path ./embassy-executor/Cargo.toml --features metadata-name
cargo test --manifest-path ./embassy-futures/Cargo.toml
cargo test --manifest-path ./embassy-sync/Cargo.toml
cargo test --manifest-path ./embassy-embedded-hal/Cargo.toml
cargo test --manifest-path ./embassy-hal-internal/Cargo.toml
cargo test --manifest-path ./embassy-time/Cargo.toml --features mock-driver,embassy-time-queue-utils/generic-queue-8
cargo test --manifest-path ./embassy-time-driver/Cargo.toml
cargo test --manifest-path ./embassy-ptp-driver/Cargo.toml --all-features

cargo test --manifest-path ./embassy-boot/Cargo.toml
cargo test --manifest-path ./embassy-boot/Cargo.toml --features ed25519-dalek
cargo test --manifest-path ./embassy-boot/Cargo.toml --features ed25519-salty

cargo test --manifest-path ./embassy-nrf/Cargo.toml --no-default-features --features nrf52840,time-driver-rtc1,gpiote

cargo test --manifest-path ./embassy-rp/Cargo.toml --no-default-features --features time-driver,rp2040,_test
cargo test --manifest-path ./embassy-rp/Cargo.toml --no-default-features --features time-driver,rp235xa,_test

cargo test --manifest-path ./embassy-stm32/Cargo.toml --no-default-features --features stm32f429vg,time-driver-any,exti,single-bank,low-power,chrono,test
cargo test --manifest-path ./embassy-stm32/Cargo.toml --no-default-features --features stm32f429vg,time-driver-any,exti,dual-bank,test
cargo test --manifest-path ./embassy-stm32/Cargo.toml --no-default-features --features stm32f732ze,time-driver-any,exti,test
cargo test --manifest-path ./embassy-stm32/Cargo.toml --no-default-features --features stm32f769ni,time-driver-any,exti,single-bank,test
cargo test --manifest-path ./embassy-stm32/Cargo.toml --no-default-features --features stm32f769ni,time-driver-any,exti,dual-bank,test

cargo test --manifest-path ./embassy-net-adin1110/Cargo.toml
cargo test --manifest-path ./embassy-usb-dfu/Cargo.toml --features dfu
cargo test --manifest-path ./embassy-usb-host/Cargo.toml
cargo test --manifest-path ./embassy-net/Cargo.toml --features tcp,dhcpv4,medium-ethernet,proto-ipv6
# Keep the optional egress-scheduling bridge honest in both directions. The
# feature-off check catches accidental references to optional driver types;
# the feature-on check exercises the complete Xarxa/driver adapter schema.
cargo check --manifest-path ./embassy-net/Cargo.toml --no-default-features --features medium-ethernet,proto-ipv4,udp
cargo check --manifest-path ./embassy-net-driver/Cargo.toml --no-default-features
cargo check --manifest-path ./embassy-net-driver/Cargo.toml --no-default-features --features tx-egress-metadata
cargo check --manifest-path ./embassy-net/Cargo.toml --no-default-features --features medium-ethernet,proto-ipv4,udp,tx-egress-metadata,iface-egress-key-count-16
