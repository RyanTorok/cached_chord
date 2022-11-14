use std::fs::File;
use std::io::BufRead;
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::path::Path;
use std::process::{Child, Command, exit, Stdio};
use clap::Parser;
use rand::seq::index::sample;
use chrono::prelude::*;
use std::time::Duration;


pub const MASTER_ID: u32 = 0;
pub const LOGS_DIR: &str = "logs";

/// Simulation of the Chord protocol with caching.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {

    /// Number of nodes
    #[arg(short)]
    n: u32,

    #[arg(short, long)]
    keys: u64,

    /// Type of cache [none, lru, mru, fifo, lifo, mfu]
    #[arg(short, long)]
    cache: String,

    /// Size of cache (entries)
    #[arg(long)]
    cache_size: usize,

    /// Number of requests
    #[arg(short, long)]
    requests: u64,

    /// Distribution of requests for nodes [uniform, zipf]
    #[arg(short, long)]
    distribution: String,

    #[arg(long, default_value_t = 1.0)]
    zipf_param: f64,

    #[arg(long, default_value_t = 25)]
    rtt: u64,

    #[arg(short, default_value_t = false)]
    verbose: bool

}

fn main() {
    let args = Args::parse();
    if args.n < 1 {
        eprintln!("Sim error: Number of nodes must be > 1");
        exit(-1);
    }
    let cap_err = "Sim error: `usize` should not be smaller than u32.";
    let mut rand = rand::thread_rng();
    let mut rand_indices = sample(&mut rand, u32::MAX.try_into().expect(cap_err), (args.n - 1).try_into().unwrap()).into_vec();
    rand_indices.sort_unstable();
    let mut rand_indices = rand_indices.iter();
    let local = Local::now();
    let out_dir = format!("sim_{}", local.format("%Y-%m-%d %H:%M:%S").to_string());
    std::fs::create_dir(Path::new(LOGS_DIR).join(Path::new(&out_dir)))
        .expect("Sim error: Could not create directory for output files because it already exists.");

    // Spawn master node
    let m_out_path = Path::new(LOGS_DIR).join(Path::new(&out_dir)).join(format!("node_{}.out", MASTER_ID));
    let mut m_out_file = File::create(m_out_path.clone()).expect("Could not create node stdout file.");
    let m_err_file = File::create(Path::new(LOGS_DIR).join(Path::new(&out_dir)).join(format!("node_{}.err", MASTER_ID))).expect("Could not create node stderr file.");
    let mut master = Command::new("mm-delay").args([
        (args.rtt / 2).to_string().as_str(),
        "sh", "-c", &format!("hostname -I; ./chord -n {} --keys {} --cache {} --cache-size {} --master-ip 0.0.0.0 --requests {} --distribution {} --zipf-param {} {}",
                            MASTER_ID.to_string().as_str(), args.keys, args.cache.as_str(), args.cache_size.to_string().as_str(), (args.requests / args.n as u64).to_string().as_str(), args.distribution.as_str(), args.zipf_param.to_string().as_str(), if args.verbose {"-v"} else {""}),
    ]).stdout( Stdio::piped()).stderr(unsafe { Stdio::from_raw_fd(m_err_file.as_raw_fd()) }).spawn().expect(&format!("Sim error: Could not spawn child process for node {}", MASTER_ID));


    // Read master IP address
    let mut master_ip = String::with_capacity(20);
    // We need a new FD, since we're already using the other one to pipe output asynchronously.
    let master_stdout = master.stdout.as_mut().expect("Could not read master stdout.");
    let mut ip_reader = std::io::BufReader::new(master_stdout);
    ip_reader.read_line(&mut master_ip).expect("Could not read master IP address from log file.");
    let master_ip = master_ip.trim();

    // Spawn all other nodes
    let mut children: Vec<(Child, u32)> = (0..args.n - 1).map(|_| {
        std::thread::sleep(Duration::from_millis(250));
        let next_id: usize = *rand_indices.next().expect("Not enough IDs. This is a bug.");
        let next_id: u32 = next_id.try_into().expect("Node ID too large. This is a bug.");

        let out_file = File::create(Path::new(LOGS_DIR).join(Path::new(&out_dir)).join(format!("node_{}.out", next_id))).expect("Could not create node stdout file.");
        let err_file = File::create(Path::new(LOGS_DIR).join(Path::new(&out_dir)).join(format!("node_{}.err", next_id))).expect("Could not create node stderr file.");

        let mut command = Command::new("mm-delay");
        let mut command = command.args([
            (args.rtt / 2).to_string().as_str(),
            "./chord",
            "-n", next_id.to_string().as_str(),
            "--keys", args.keys.to_string().as_str(),
            "--cache", args.cache.as_str(),
            "--cache-size", args.cache_size.to_string().as_str(),
            "--master-ip", master_ip,
            "--requests", (args.requests / args.n as u64).to_string().as_str(),
            "--distribution", args.distribution.as_str(),
            "--zipf-param", args.zipf_param.to_string().as_str()
        ]);
        if args.verbose {
            command = command.arg("-v");
        }
        (command.stdout(unsafe { Stdio::from_raw_fd(out_file.as_raw_fd()) }).stderr(unsafe { Stdio::from_raw_fd(err_file.as_raw_fd()) } ).spawn().expect(&format!("Sim error: Could not spawn child process for node {}", next_id)), next_id)
    }).collect();
    //master.wait().expect("Master panicked.");
    std::io::copy(&mut master.stdout.unwrap(), &mut m_out_file).expect("Failed to pipe master output to outfile.");
    children.iter_mut().for_each(|c| {
        c.0.wait().expect("Normal node panicked.");
    });
}
