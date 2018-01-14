extern crate byteorder;
extern crate clap;
#[macro_use]
extern crate lazy_static;
extern crate memmap;
extern crate parking_lot;
extern crate quantiles;
extern crate rand;

use parking_lot::Mutex;
use std::{fs, mem, panic, sync, thread, time};
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::path::PathBuf;
use rand::Rng;
use clap::{App, Arg, ArgGroup};
use std::io::{Cursor, Seek, SeekFrom, Write};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use quantiles::ckms::CKMS;

lazy_static! {
    static ref VALUES_TO_READ: sync::Arc<AtomicUsize> = sync::Arc::new(AtomicUsize::new(0));
}

static PATH: &'static str = "/tmp/mmap_comm";

pub fn delay(attempts: u32) -> () {
    use std::time;
    let delay = match attempts {
        0 => 0,
        1 => 1,
        2 => 4,
        3 => 8,
        4 => 16,
        _ => 32,
    };
    let sleep_time = time::Duration::from_millis(delay as u64);
    thread::sleep(sleep_time);
}

fn receiver(cap: usize) -> (u64, u64) {
    let path: PathBuf = PathBuf::from(PATH);
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)
        .unwrap();
    file.set_len((mem::size_of::<u64>() * cap) as u64).unwrap();
    let mmap = unsafe { memmap::MmapMut::map_mut(&file).unwrap() };

    let mut total_read = 0;
    let mut summation: u64 = 0;
    let mut offset = 0;
    let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::with_capacity(8));
    buf.seek(SeekFrom::Start(0)).unwrap();

    let mut attempts = 0;
    let sz = mem::size_of::<u64>();
    while total_read < cap {
        let mut values_to_read = VALUES_TO_READ.load(Ordering::Relaxed);
        let vtr_sub = values_to_read;
        while values_to_read != 0 {
            attempts = 0;
            buf.seek(SeekFrom::Start(0)).unwrap();
            buf.write_all(&mmap[offset..offset + sz]).unwrap();
            buf.seek(SeekFrom::Start(0)).unwrap();
            offset += sz;

            let val = buf.read_u64::<BigEndian>().unwrap();
            assert!(0 != val);
            summation += val;
            total_read += 1;
            values_to_read -= 1;
        }
        if vtr_sub != 0 {
            VALUES_TO_READ.fetch_sub(vtr_sub, Ordering::AcqRel);
        }
        delay(attempts);
        attempts += 1;
    }
    (total_read as u64, summation)
}

struct SenderControl {
    offset: usize,
    unflushed_writes: usize,
}

fn sender(cap: usize, sender_cap: usize, lock: sync::Arc<Mutex<SenderControl>>) -> (u64, u64) {
    let path: PathBuf = PathBuf::from(PATH);
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)
        .unwrap();
    file.set_len((mem::size_of::<u64>() * cap) as u64).unwrap();
    let mut mmap = unsafe { memmap::MmapMut::map_mut(&file).unwrap() };

    let mut rng = rand::thread_rng();

    let flush_limit = 1_000;

    let mut total_written: usize = 0;
    let mut idx = 0;
    let sz = mem::size_of::<u64>();
    let mut summation: u64 = 0;
    let mut buf = vec![]; // TODO can we write directly to mmap?
    while idx < sender_cap {
        let val = u64::from(rng.gen::<u8>()) + 1; // don't want to overflow...
        assert!(val != 0);
        summation += val;
        buf.write_u64::<BigEndian>(val)
            .expect("SERIALIZING THE VAL");
        let mut control = lock.lock();
        for byte in buf.iter().take(sz) {
            mmap[(*control).offset] = *byte;
            (*control).offset += 1;
        }
        buf.clear();
        (*control).unflushed_writes += 1;
        if (*control).unflushed_writes > flush_limit {
            mmap.flush().expect("could not flush");
            VALUES_TO_READ.fetch_add((*control).unflushed_writes, Ordering::Relaxed);
            total_written += (*control).unflushed_writes;;
            (*control).unflushed_writes = 0;
        }
        idx += 1;
    }
    let mut control = lock.lock();
    mmap.flush().expect("could not flush");
    total_written += (*control).unflushed_writes;
    VALUES_TO_READ.fetch_add((*control).unflushed_writes, Ordering::Relaxed);
    (*control).unflushed_writes = 0;

    (total_written as u64, summation)
}

fn experiment(total_senders: usize, cap: usize) -> ((u64, u64), (u64, u64)) {
    // reset experimental clean-room conditions
    VALUES_TO_READ.store(0, Ordering::SeqCst);
    let _ = fs::remove_file(PATH);

    let sender_cap = cap / total_senders;
    let sender_lock = sync::Arc::new(Mutex::new(SenderControl {
        offset: 0,
        unflushed_writes: 0,
    }));

    let receiver_join = thread::spawn(move || receiver(cap));
    let mut senders = vec![];
    for _ in 0..total_senders {
        let lk = sender_lock.clone();
        senders.push(thread::spawn(move || sender(cap, sender_cap, lk)))
    }
    drop(sender_lock);

    let mut sender_sum = 0;
    let mut sender_written = 0;
    for sender_join in senders {
        let (indv_send_written, indv_send_sum) = sender_join.join().unwrap();
        sender_sum += indv_send_sum;
        sender_written += indv_send_written;
    }
    let rcv = receiver_join.join().unwrap();

    (rcv, (sender_written, sender_sum))
}

fn validate(rcv: (u64, u64), snd: (u64, u64)) -> () {
    let (receiver_read, receiver_sum) = rcv;
    let (sender_written, sender_sum) = snd;

    assert_eq!(receiver_read, sender_written);

    assert!(sender_sum >= receiver_sum);
    assert_eq!(sender_sum, receiver_sum);
}

fn main() {
    // It's possible that someone will run this on a u32 system. Until AtomicU64
    // becomes stable we are limited to running this friendly program on 64 bit
    // systems.
    assert_eq!(mem::size_of::<usize>(), mem::size_of::<u64>());

    let matches = App::new("mmap_comm")
        .arg(
            Arg::with_name("total_writes")
                .long("total_writes")
                .value_name("INT")
                .help("maximum values to write to the mmap'ed file")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("total_senders")
                .long("total_senders")
                .value_name("INT")
                .help("total number of sender threads to use")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("max_experiments")
                .long("max_experiments")
                .value_name("INT")
                .help("maximum number of experiments to run")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("duration")
                .long("duration")
                .value_name("INT")
                .help("length of time in seconds to run experiments for")
                .takes_value(true),
        )
        .group(
            ArgGroup::with_name("experimental_controls")
                .args(&["duration", "max_experiments"])
                .required(true),
        )
        .get_matches();

    let mut successes = 0;
    let mut failures = 0;
    let mut times = CKMS::new(0.001);

    let cap = usize::from_str(matches.value_of("total_writes").unwrap()).unwrap();
    let total_senders = usize::from_str(matches.value_of("total_senders").unwrap()).unwrap();
    let start = time::Instant::now();

    if let Some(max_experiments) = matches.value_of("max_experiments") {
        let mut max_experiments = usize::from_str(max_experiments).unwrap();
        while max_experiments > 0 {
            let result = panic::catch_unwind(|| {
                let now = time::Instant::now();
                let (recv, snd) = experiment(total_senders, cap);
                let elapsed = now.elapsed();
                validate(recv, snd);
                elapsed
            });
            if let Ok(elapsed) = result {
                times.insert(
                    (elapsed.as_secs() as f64) + (elapsed.subsec_nanos() as f64 / 1000_000_000.0),
                );
                successes += 1;
            } else {
                failures += 1;
            }
            max_experiments -= 1;
        }
    } else if let Some(duration) = matches.value_of("duration") {
        let limit = time::Duration::from_secs(u64::from_str(duration).unwrap());
        loop {
            let result = panic::catch_unwind(|| {
                let now = time::Instant::now();
                let (recv_sum, snd_sum) = experiment(total_senders, cap);
                let elapsed = now.elapsed();
                validate(recv_sum, snd_sum);
                elapsed
            });
            if let Ok(elapsed) = result {
                times.insert(
                    (elapsed.as_secs() as f64) + (elapsed.subsec_nanos() as f64 / 1000_000_000.0),
                );
                successes += 1;
            } else {
                failures += 1;
            }
            if start.elapsed() > limit {
                break;
            }
        }
    } else {
        println!("Oops please give duration or max_experiments");
        unreachable!()
    }

    println!("FAILURES: {}", failures);
    println!("SUCCESSES: {}", successes);
    println!("ELAPSED: {:?}", start.elapsed());
    println!("TIMES (of {}): ", times.count());
    println!("    MIN: {:.4}", times.query(0.0).unwrap().1);
    println!("    AVG: {:.4}", times.cma().unwrap());
    println!("    0.75: {:.4}", times.query(0.75).unwrap().1);
    println!("    0.90: {:.4}", times.query(0.90).unwrap().1);
    println!("    0.95: {:.4}", times.query(0.95).unwrap().1);
    println!("    MAX: {:.4}", times.query(1.0).unwrap().1);
}
