extern crate byteorder;
extern crate coco;
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
    static ref VALUES_TO_READ: sync::Arc<AtomicUsize> = sync::Arc::new(AtomicUsize::new(0));
    static ref SENDER_Q: coco::Stack<Action> = coco::Stack::new();
    static ref RECEIVER_Q: coco::Stack<Action> = coco::Stack::new();
}

static CAP: usize = 10_000;
static PATH: &'static str = "/tmp/mmap_comm";

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Action {
    Write { offset: usize, val: u64 },
    Read { offset: usize, val: u64 },
}

impl Action {
    fn is_write(&self) -> bool {
        match *self {
            Action::Write { .. } => true,
            _ => false,
        }
    }

    fn is_read(&self) -> bool {
        match *self {
            Action::Read { .. } => true,
            _ => false,
        }
    }
}

pub fn delay(attempts: u32) -> Result<usize, ()> {
    use std::time;
    let delay = match attempts {
        0 => 0,
        1 => 1,
        2 => 4,
        3 => 8,
        4 => 16,
        5 => 32,
        6 => 64,
        7 => 128,
        8 => 256,
        9 => 512,
        10 => return Err(()),
        _ => unreachable!(),
    };
    let sleep_time = time::Duration::from_millis(delay as u64);
    thread::sleep(sleep_time);
    Ok(delay)
}

fn receiver() -> u64 {
    let path: PathBuf = PathBuf::from(PATH);
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)
        .unwrap();
    file.set_len((mem::size_of::<u64>() * CAP) as u64).unwrap();
    let mmap = unsafe { memmap::MmapMut::map_mut(&file).unwrap() };

    let mut summation: u64 = 0;
    let mut offset = 0;
    let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::with_capacity(8));
    buf.seek(SeekFrom::Start(0)).unwrap();

    let mut attempts = 0;
    loop {
        while VALUES_TO_READ.load(Ordering::SeqCst) != 0 {
            buf.seek(SeekFrom::Start(0)).unwrap();
            buf.write_all(&mmap[offset..offset + 8]).unwrap();
            buf.seek(SeekFrom::Start(0)).unwrap();
            offset += 8;

            let val = buf.read_u64::<BigEndian>().unwrap();
            RECEIVER_Q.push(Action::Read {
                offset: offset - 8,
                val: val,
            });
            summation += val;

            VALUES_TO_READ.fetch_sub(1, Ordering::SeqCst);
        }
        if let Err(_) = delay(attempts) {
            break;
        } else {
            attempts += 1;
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
    file.set_len((mem::size_of::<u64>() * CAP) as u64).unwrap();
    let mut mmap = unsafe { memmap::MmapMut::map_mut(&file).unwrap() };

    let mut rng = rand::thread_rng();

    let mut offset = 0;
    let mut idx = 0;
    let mut summation: u64 = 0;
    let mut buf = vec![]; // TODO can we write directly to mmap?
    while idx < CAP {
        let val = u64::from(rng.gen::<u8>()); // don't want to overflow...
        summation += val;
        buf.write_u64::<BigEndian>(val)
            .expect("SERIALIZING THE VAL");
        SENDER_Q.push(Action::Write {
            offset: offset,
            val: val,
        });
        for byte in buf.iter().take(8) {
            mmap[offset] = *byte;
            offset += 1;
        }
        buf.clear();
        VALUES_TO_READ.fetch_add(1, Ordering::SeqCst);
        idx += 1;
    }

    summation
}

fn experiment() {
    let _ = fs::remove_file(PATH);

    let sender_join = thread::spawn(sender);
    let receiver_join = thread::spawn(receiver);

    let sender_sum = sender_join.join().unwrap();
    let receiver_sum = receiver_join.join().unwrap();

    let mut sender_actions: Vec<Action> = Vec::new();
    while let Some(action) = SENDER_Q.pop() {
        assert!(action.is_write());
        if let Err(idx) = sender_actions.binary_search(&action) {
            sender_actions.insert(idx, action)
        } else {
            unreachable!()
        }
    }
    let mut receiver_actions: Vec<Action> = Vec::new();
    while let Some(action) = RECEIVER_Q.pop() {
        assert!(action.is_read());
        if let Err(idx) = receiver_actions.binary_search(&action) {
            receiver_actions.insert(idx, action)
        } else {
            unreachable!()
        }
    }

    assert_eq!(sender_actions.len(), receiver_actions.len());
    for tup in sender_actions.iter().zip(receiver_actions.iter()) {
        match tup {
            (
                &Action::Write {
                    offset: lo,
                    val: lv,
                },
                &Action::Read {
                    offset: ro,
                    val: rv,
                },
            ) => {
                if (lo != ro) || (lv != rv) {
                    panic!("DIFFERED ON OPERATION PAIR: {:?}", tup);
                }
            }
            _ => unreachable!(),
        }
    }

    assert_eq!(sender_sum, receiver_sum);
}

fn main() {
    // It's possible that someone will run this on a u32 system. Until AtomicU64
    // becomes stable we are limited to running this friendly program on 64 bit
    // systems.
    assert_eq!(mem::size_of::<usize>(), mem::size_of::<u64>());

    loop {
        experiment()
    }
}
