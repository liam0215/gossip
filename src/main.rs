use clap::Parser;
use parking_lot::Mutex;
use std::{
    cmp::Ordering,
    collections::HashMap,
    io::stdin,
    io::Write,
    net::{Ipv4Addr, SocketAddrV4},
    str::FromStr,
    sync::Arc,
    thread,
    time::{Duration, SystemTime},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration as TokioDuration};

type Port = u16;
type Db = Arc<Mutex<HashMap<Ipv4Addr, Vec<Node>>>>;
type BlackList = Arc<Mutex<Vec<(Ipv4Addr, Port)>>>;

#[derive(Parser, Debug)]
#[clap()]
struct Args {
    /// IP address of this program
    #[clap(short, long, value_parser)]
    ip: Ipv4Addr,

    /// Port number of this program
    #[clap(short, long, value_parser, default_value_t = 5000)]
    port: u16,
}

#[derive(Debug, Clone)]
struct Node {
    ip: Ipv4Addr,
    port: Port,
    time: SystemTime,
    value: u8,
}

enum _UserInput {
    AddNode(Ipv4Addr, Port),
    ChangeValue(u8),
    GetNodeList,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let self_addr = SocketAddrV4::new(args.ip, args.port);

    // Bind the listener to the address
    let listener = TcpListener::bind(self_addr).await.unwrap();

    let db: Db = Db::new(Mutex::new(HashMap::new()));

    let black_list: BlackList = BlackList::new(Mutex::new(Vec::new()));

    let (tx, mut rx) = mpsc::channel::<(Ipv4Addr, Port)>(16);

    {
        let db = db.clone();
        let self_addr = self_addr.clone();
        thread::spawn(move || {
            process_input(db, self_addr, tx);
        });
    }

    {
        let db = db.clone();
        let black_list = black_list.clone();
        let self_addr = self_addr.clone();
        tokio::spawn(async move {
            loop {
                while let Some(message) = rx.recv().await {
                    let db = db.clone();
                    let black_list = black_list.clone();
                    poll_node(db, message.0, message.1, self_addr, black_list).await;
                }
            }
        });
    }

    {
        let db = db.clone();
        let black_list = black_list.clone();
        let self_addr = self_addr.clone();
        tokio::spawn(async move {
            loop {
                let mut maybe_socket: Option<SocketAddrV4> = None;
                {
                    let db = db.lock();
                    for _ in 0..5 {
                        let random_index = rand::random::<usize>() % db.len();
                        let (ip, nodes) = db.iter().nth(random_index).unwrap();
                        let random_index = rand::random::<usize>() % nodes.len();
                        let node = nodes.get(random_index).unwrap();
                        if *ip != *self_addr.ip() || node.port != self_addr.port() {
                            maybe_socket = Some(SocketAddrV4::new(node.ip, node.port));
                            break;
                        }
                    }
                }
                if let Some(socket) = maybe_socket {
                    let db = db.clone();
                    let black_list = black_list.clone();
                    poll_node(db, *socket.ip(), socket.port(), self_addr, black_list).await;
                }
                sleep(TokioDuration::from_secs(3)).await;
            }
        });
    }

    loop {
        // The second item contains the IP and port of the new connection.
        let (socket, _) = listener.accept().await.unwrap();

        // Clone the handle to the hash map.
        let db = db.clone();

        // A new task is spawned for each inbound socket. The socket is
        // moved to the new task and processed there.
        tokio::spawn(async move {
            process(socket, db).await;
        });
    }
}

async fn process(mut socket: TcpStream, db: Db) {
    let mut buf: Vec<u8> = vec![];
    {
        let db = db.lock();
        for (key, value) in db.iter() {
            for node in value.iter() {
                let Node {
                    ip: _,
                    port,
                    time,
                    value,
                } = node;
                buf.extend_from_slice(key.to_string().as_bytes());
                buf.push(b':');
                buf.extend_from_slice(port.to_string().as_bytes());
                buf.push(b',');
                let time_since_unix_epoch = time
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                buf.extend(time_since_unix_epoch.to_string().as_bytes());
                buf.push(b',');
                buf.extend_from_slice(value.to_string().as_bytes());
                buf.push(0xA);
            }
        }
    }
    socket
        .write_all(&buf)
        .await
        .unwrap_or_else(|err| println!("Error writing to socket: {}", err));
    socket
        .shutdown()
        .await
        .unwrap_or_else(|err| println!("Error shutting down socket: {}", err));
}

fn process_input(db: Db, self_addr: SocketAddrV4, tx: mpsc::Sender<(Ipv4Addr, Port)>) {
    let mut buf = String::new();
    loop {
        print!(">> ");
        std::io::stdout().flush().expect("Failed to flush stdout");
        stdin().read_line(&mut buf).expect("Failed to read line");
        let input = buf.trim();
        let split_input: Vec<&str> = input.split(":").collect();
        match split_input[0].chars().next() {
            Some('?') => {
                let db = db.lock();
                for (key, value) in db.iter() {
                    for node in value.iter() {
                        let Node {
                            ip: _,
                            port,
                            time: _,
                            value,
                        } = node;
                        print_node_value(key.to_string() + ":" + &port.to_string(), *value);
                    }
                }
            }
            Some('!') => {
                println!("{:?}", db.lock());
            }
            Some('+') => match Ipv4Addr::from_str(split_input[0][1..].trim()) {
                Ok(ip) => match split_input.get(1) {
                    Some(port) if port.parse::<Port>().is_ok() => {
                        let port = port.parse::<Port>().unwrap();
                        if let Err(e) = tx.try_send((ip, port)) {
                            println!("Error sending message to poll_node thread: {}", e);
                        }
                    }
                    Some(_) => println!("Error: Could not parse port"),
                    None => println!("Error: missing port"),
                },
                Err(_) => {
                    println!("Could not parse IP address");
                }
            },
            Some(n) if n.is_digit(10) => {
                let value = n.to_digit(10).unwrap() as u8;
                {
                    let mut db = db.lock();
                    match db.get_mut(self_addr.ip()) {
                        Some(self_node) => {
                            let mut found_node = false;
                            for node in self_node {
                                if node.port == self_addr.port() {
                                    node.value = value;
                                    node.time = SystemTime::now();
                                    found_node = true;
                                    print_node_value(self_addr.to_string(), value);
                                }
                            }
                            if !found_node {
                                println!("Could not find self in db under key: {}", self_addr.ip());
                            }
                        }
                        None => {
                            let new_self_node = Node {
                                ip: *self_addr.ip(),
                                port: self_addr.port(),
                                time: SystemTime::now(),
                                value,
                            };
                            db.insert(*self_addr.ip(), vec![new_self_node]);
                            print_node_value(self_addr.to_string(), value);
                        }
                    }
                }
            }
            Some(_) => {
                println!("Invalid input, please input +<ip>:<port>, ?, !, or a digit");
            }
            None => {
                println!("No input read, please try again");
            }
        }
        buf.clear();
    }
}

async fn poll_node(
    db: Db,
    ip: Ipv4Addr,
    port: Port,
    self_addr: SocketAddrV4,
    black_list: BlackList,
) {
    {
        let black_list = black_list.lock();
        if black_list.contains(&(ip, port)) {
            return;
        }
    }
    let socket_result = TcpStream::connect((ip, port)).await;
    let mut socket = match socket_result {
        Ok(socket) => socket,
        Err(_) => {
            let mut black_list = black_list.lock();
            black_list.push((ip, port));
            let mut db = db.lock();
            if let Some(nodes) = db.get_mut(&ip) {
                nodes.retain(|node| node.port != port);
            }
            return;
        }
    };
    let mut buf: String = String::new();
    socket.read_to_string(&mut buf).await.unwrap();
    buf.retain(|c| !c.is_whitespace() || c == '\n');
    let mut lines: Vec<&str> = buf.split('\n').collect();
    lines = lines
        .iter()
        .filter(|line| !line.is_empty())
        .map(|line| *line)
        .collect();
    println!();
    for line in lines {
        let split_line: Vec<&str> = line.split(',').collect();
        let peer_socket = match SocketAddrV4::from_str(split_line[0]) {
            Ok(p_socket) => p_socket,
            Err(_) => {
                println!("Error parsing socket address: {}", split_line[0]);
                continue;
            }
        };

        if peer_socket == self_addr {
            continue;
        }

        let time = match split_line[1].parse::<u64>() {
            Ok(time) => SystemTime::UNIX_EPOCH + Duration::from_secs(time),
            Err(_) => {
                println!("Error parsing time: {}", split_line[1]);
                continue;
            }
        };

        if SystemTime::now().duration_since(time).is_err() {
            continue;
        }

        let value = match split_line[2].parse::<u8>() {
            Ok(value) => value,
            Err(_) => {
                println!("Error parsing value: {}", split_line[2]);
                continue;
            }
        };
        {
            let node = Node {
                ip: *peer_socket.ip(),
                port: peer_socket.port(),
                time,
                value,
            };

            let mut db = db.lock();
            if let Some(nodes) = db.get_mut(&node.ip) {
                let mut found_node = false;

                for mut n in nodes.iter_mut() {
                    if n.port == peer_socket.port() {
                        if time.duration_since(n.time).is_ok() {
                            n.time = time;
                            n.value = value;
                            print_node_value(peer_socket.to_string(), value);
                        }
                        found_node = true;
                    }
                }

                if !found_node {
                    nodes.push(node);
                    if nodes.len() > 3 {
                        nodes.sort_by(|a, b| {
                            if a.ip == *self_addr.ip() && a.port == self_addr.port() {
                                Ordering::Greater
                            } else if b.ip == *self_addr.ip() && b.port == self_addr.port() {
                                Ordering::Less
                            } else {
                                a.time.cmp(&b.time)
                            }
                        });
                        nodes.reverse();
                        nodes.truncate(3);
                    }
                    print_node_value(peer_socket.to_string(), value);
                }
            } else {
                db.insert(ip, vec![node]);
                print_node_value(peer_socket.to_string(), value);
            }
        }
    }
    print!(">> ");
    std::io::stdout().flush().expect("Failed to flush stdout");
}

fn print_node_value(node: String, value: u8) {
    println!("{} --> {}", node, value);
}
