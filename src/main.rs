extern crate byteorder;
#[macro_use]
extern crate lazy_static;
extern crate memmap;
extern crate rand;

use std::{fs, mem, sync, thread};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::path::PathBuf;
use rand::Rng;
use std::io::{Cursor, Seek, SeekFrom, Write};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

lazy_static! {
    static ref TOTAL_SENDERS: sync::Arc<AtomicUsize> = sync::Arc::new(AtomicUsize::new(0));
    static ref VALUES_TO_READ: sync::Arc<AtomicUsize> = sync::Arc::new(AtomicUsize::new(0));
}

static PATH: &'static str = "/tmp/mmap_comm";

fn receiver() -> u64 {
    let path: PathBuf = PathBuf::from(PATH);
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)
        .unwrap();
    file.set_len((mem::size_of::<u64>() * 10_000) as u64)
        .unwrap();
    let mmap = unsafe { memmap::MmapMut::map_mut(&file).unwrap() };

    let mut summation: u64 = 0;
    let mut offset = 0;
    let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::with_capacity(8));
    buf.seek(SeekFrom::Start(0)).unwrap();

    loop {
        while VALUES_TO_READ.load(Ordering::SeqCst) != 0 {
            buf.write_all(&mmap[offset..offset + 8]).unwrap();
            buf.seek(SeekFrom::Start(0)).unwrap();
            offset += 8;

            let val = buf.read_u64::<BigEndian>().unwrap();
            summation += val;

            VALUES_TO_READ.fetch_sub(1, Ordering::SeqCst);
        }
        // TODO this synchronization is crummy and totally racey. Fun race
        // though.
        if TOTAL_SENDERS.load(Ordering::SeqCst) == 0 {
            break;
        }
    }
    summation
}

fn sender() -> u64 {
    let path: PathBuf = PathBuf::from(PATH);
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)
        .unwrap();
    file.set_len((mem::size_of::<u64>() * 10_000) as u64)
        .unwrap();
    let mut mmap = unsafe { memmap::MmapMut::map_mut(&file).unwrap() };

    TOTAL_SENDERS.fetch_add(1, Ordering::SeqCst);

    let mut rng = rand::thread_rng();

    let mut offset = 0;
    let mut idx = 0;
    let mut summation: u64 = 0;
    let mut buf = vec![]; // TODO can we write directly to mmap?
    while idx < 10_000 {
        let val = rng.gen::<u8>() as u64; // don't want to overflow...
        summation += val;
        buf.write_u64::<BigEndian>(val)
            .expect("SERIALIZING THE VAL");
        for i in 0..8 {
            mmap[offset] = buf[i];
            offset += 1;
        }
        // nothing intentionally
        VALUES_TO_READ.fetch_add(1, Ordering::SeqCst);
        idx += 1;
    }

    TOTAL_SENDERS.fetch_sub(1, Ordering::SeqCst);

    summation
}

fn main() {
    let _ = fs::remove_file(PATH);

    let sender_join = thread::spawn(sender);
    let receiver_join = thread::spawn(receiver);

    let sender_sum = sender_join.join().unwrap();
    let receiver_sum = receiver_join.join().unwrap();

    assert_eq!(sender_sum, receiver_sum);
}
