use std::path::{Path};
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::collections::HashMap;
use clap::{Parser};
use log::{info, LevelFilter, warn};
use std::sync::{Arc};
use std::{thread};
use std::io::{BufReader, BufWriter};
use std::thread::available_parallelism;
use std::iter::repeat_with;
use fastrand;
#[derive(Parser)]
#[command(name = "csvgen")]
#[command(about = "Generate CSV with random Fields and Data", long_about = None)]
#[command(author, version)]
struct Args {
    #[arg(short, long)]
    lines: u32,
    #[arg(short, long)]
    cols: u16,
    #[arg(short, long, default_value = "~|")]
    delimiter: String,
    #[arg(short, long, default_value = "genfile")]
    filename:String,
    #[arg(short, long)]
    threads: u32
}



fn data_string_builder(columns_map: &HashMap<String, &str>, del: &[u8]) -> Vec<u8> {
    let mut write_bytes = Vec::new();
    for value in columns_map.values() {
        if value == &"VARCHAR(100)" {
            let temp: String = repeat_with(fastrand::alphanumeric).take(10).collect();;
            write_bytes.extend(temp.as_bytes());
        } else if value == &"NUMBER(10,0)" {
            let num: i32 = fastrand::i32(0..12000);
            write_bytes.extend(format!("{}", num).as_bytes());
        } else {
            let day: i32 = fastrand::i32(1..=25);
            let month: i32 = fastrand::i32(1..=12);
            let year: i32 = fastrand::i32(1960..=2024);
            write_bytes.extend(format!("{}-{}-{}", day, month, year).as_bytes());
        }
        write_bytes.extend(del);
    }

    if !write_bytes.is_empty() {
        write_bytes.truncate(write_bytes.len() - del.len());
    }

    write_bytes
}

fn consolidate_files(temp_dir_path: &Path, main_file: File) -> std::io::Result<()> {

    let mut main_file_writer = BufWriter::new(main_file);
    for entry in std::fs::read_dir(temp_dir_path)? {
        info!("Merging Thread Files:");
        let entry = entry?;
        let temp_file = File::open(entry.path())?;
        let mut temp_reader = BufReader::new(temp_file);
        let by = std::io::copy(&mut temp_reader, &mut main_file_writer)?; //std::io::copy is theoretically faster, but might just not be fast enough
        info!("{} bytes written from {:?}",by, entry.file_name());
    }

    Ok(())
}
fn main() {
    println!("CSVGen!");
    env_logger::builder().filter_level(LevelFilter::Info).init();
    let mut args = Args::parse();
    info!("Lines: {:?}, \n Columns {:?}\n Delimiter: {:?}\n Filename: {:?}", &args.lines,&args.cols, &args.delimiter,&args.filename);
    if(args.lines < 100 || args.lines / args.threads <= 10 ) {
        warn!("Forcing single thread as MAX_BUFFER per thread is set as 10, so file will be empty");
        args.threads = 1;
    }
    let max_threads_supported = available_parallelism().expect("Can't query number of threads!");
    info!("Max Threads Supported [Queried as per permissions available by the program]: {}",max_threads_supported);
    if args.threads > max_threads_supported.get() as u32 {
        warn!("More threads specified than allowed, performance might be slower. Consider matching the number displayed above.")
    }
    let temp_dir = tempfile::Builder::new().prefix("temp_files").tempdir().expect("Failed to create temporary directory");
    let temp_dir_path = temp_dir.path();
    info!("Temp Directory Created at: {:?}",temp_dir_path.to_str());
    let mut columns_map: HashMap<String,&str> = HashMap::new();
    let mut column_line: String = String::new();
    for i in 0..args.cols {
        let y: i32 = fastrand::i32(0..=2);
        if(y == 0) {
            columns_map.insert(format!("{}{}","FIELD",i), "VARCHAR(100)");
        } else if(y == 1) {
            columns_map.insert(format!("{}{}","FIELD",i), "NUMBER(10,0)");
        } else {
            columns_map.insert(format!("{}{}","FIELD",i), "TIMESTAMP");
        }
    }
    info!("Creating file....");
    let mut write_file = File::create(&args.filename).expect("Creation of Writer File Failed.");
    info!("File created: {:#?}",args.filename);

    {
        for (key, _) in &columns_map {
            let mut write_line: String = String::from(key.to_owned());
            write_line.push_str(&args.delimiter);
            column_line.push_str(&write_line);
        }
        column_line = column_line.trim_end_matches(&args.delimiter).to_string();
        writeln!(write_file, "{}", column_line).expect("Write FAILED");
    }
    {
        info!("::Preparing SQL String::");
        let mut sqlstr = String::new();
        for col in column_line.split(&args.delimiter) {
            sqlstr.push_str(&format!("{} {},", col, columns_map.get(col).unwrap()));
        }
        let pathstr = &args.filename;
        let mut sql_file = File::create(pathstr.to_owned() + "_SQL").expect("Creation of Writer File Failed.");
        writeln!(sql_file, "CREATE TABLE {}", sqlstr).expect("Write FAILED");
        info!("::SQL File Created::");
    }


    let colmap = Arc::new(columns_map);
    let del = Arc::new(args.delimiter);
    {
        let thread_count = args.threads;
        let mut handles = vec![];
        let tasks_per_thread = args.lines / thread_count;
        info!("Tasks per thread:{}",tasks_per_thread);

        for thread_index in 0..thread_count {
            let temp_file_path = temp_dir_path.join(format!("temp_file_{}.txt", thread_index));
            let temp_file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&temp_file_path)
                .expect("Failed to create temp file");

            let mut buffered_file = BufWriter::new(temp_file);
            let mut buffer = Vec::with_capacity(10);
            let del = Arc::clone(&del);
            let colmap = Arc::clone(&colmap);

            let handle = thread::spawn(move || {
                let start_task = thread_index * tasks_per_thread;
                let end_task = if thread_index == thread_count - 1 {
                    args.lines
                } else {
                    (thread_index + 1) * tasks_per_thread
                };

                for task_index in start_task..end_task {
                    let data :Vec<u8> = data_string_builder(&colmap, &del.as_bytes());
                    buffer.push(data);
                    if buffer.len() >= 10 || task_index == end_task{
                        for data in &mut *buffer {
                            buffered_file.write_all(data).expect("Failed writing data to file");
                            writeln!(buffered_file, "").expect("Failed writing newline to file");
                            // writeln!(buffered_file, "{}", data).expect("Failed writing data to file");
                        }
                        buffer.clear(); // Clear buffer after writing
                    }
                }
                info!("Thread index {} tasks completed", thread_index);
            });
            handles.push(handle);
        }


        for handle in handles {
            handle.join().expect("Thread panicked");
        }
    }
    consolidate_files(temp_dir_path, write_file).expect("Failed to consolidate files");
    temp_dir.close().expect("Closed");
    info!("File Created.");
}
