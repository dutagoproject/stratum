param([string]$Daemon='http://127.0.0.1:19085',[string]$Bind='127.0.0.1:11001')
cargo run -- --bind $Bind --daemon $Daemon
