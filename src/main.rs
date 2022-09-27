use clap::Parser;
use parking_lot::Mutex;
use std::{
    cmp::Ordering,
    collections::HashMap,
    error::Error,
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

const BACKGROUND_POLLING_INTERVAL: TokioDuration = TokioDuration::from_secs(3);

#[derive(Parser, Debug)]
#[clap()]
struct Args {
    /// IP address of this node
    #[clap(short, long, value_parser, default_value = "127.0.0.1")]
    ip: Ipv4Addr,

    /// Port number of this node
    #[clap(short, long, value_parser, default_value_t = 5000)]
    port: Port,
}

#[derive(Debug, Clone, PartialEq)]
struct Node {
    port: Port,
    time: SystemTime,
    value: u8,
}

#[tokio::main]
async fn main() {
    // Parse IP and port from command line arguments
    let args = Args::parse();
    let self_addr = SocketAddrV4::new(args.ip, args.port);

    // Bind the TCP listener to the address
    let listener = TcpListener::bind(self_addr).await.unwrap();

    // Initialize DB and blacklist
    let db: Db = Db::new(Mutex::new(HashMap::new()));
    let black_list: BlackList = BlackList::new(Mutex::new(Vec::new()));

    // Initialize sender and receiver for channel communication between user input thread and node polling thread
    let (tx, rx) = mpsc::channel::<(Ipv4Addr, Port)>(16);

    {
        // Spawn native OS thread to handle (blocking) user input
        // When this thread parses a connection request from user input,
        // it sends the IP and port to the node polling thread via the channel sender (tx)
        let db = db.clone();
        let self_addr = self_addr.clone();
        thread::spawn(move || {
            process_input(db, self_addr, tx);
        });
    }

    {
        // Spawn a tokio task to handle first time node connection requests from user input
        // Listens for IP and port from the channel receiver (rx), and attempts to connect to the node
        let db = db.clone();
        let black_list = black_list.clone();
        let self_addr = self_addr.clone();
        tokio::spawn(async move {
            handle_first_time_node_connection_req(db, black_list, self_addr, rx).await;
        });
    }

    {
        // Spawns a tokio task to handle background polling
        // Polls a random node in the DB every BACKGROUND_POLLING_INTERVAL seconds
        let db = db.clone();
        let black_list = black_list.clone();
        let self_addr = self_addr.clone();
        tokio::spawn(async move {
            background_node_polling(db, black_list, self_addr).await;
        });
    }

    loop {
        // Accept incoming connection from other node
        let (socket, _) = listener.accept().await.unwrap();

        // Clone the handle to the database
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
        // For each node in the DB, serialize the node info and write it to the buffer
        for (key, value) in db.iter() {
            for node in value.iter() {
                let Node { port, time, value } = node;
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

    // Write the buffer to the socket and close the connection
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

        // Trim whitespace from input
        let input = buf.trim();

        match input.chars().next() {
            Some('?') => {
                // Print nodes and their last known value from database
                let db = db.lock();
                for (key, value) in db.iter() {
                    for node in value.iter() {
                        let Node {
                            port,
                            time: _,
                            value,
                        } = node;
                        print_node_value(key.to_string() + ":" + &port.to_string(), *value);
                    }
                }
            }
            Some('!') => {
                // For debugging purposes - print database
                println!("{:?}", db.lock());
            }
            Some('+') => {
                // Parse IP and port from input, and send to listening tokio thread to
                // attempt to connect to the node
                match parse_ip_and_port_from_input(input) {
                    Ok((ip, port)) => {
                        if let Err(e) = tx.try_send((ip, port)) {
                            println!("Error sending message to poll_node thread: {}", e);
                        }
                    }
                    Err(_) => println!("Could not parse ip and port from input"),
                }
            }
            Some(n) if n.is_digit(10) => {
                // If the input is a digit, upsert this node's value in the database
                let value = n.to_digit(10).unwrap() as u8;
                {
                    let node = Node {
                        port: self_addr.port(),
                        time: SystemTime::now(),
                        value,
                    };

                    let db = db.clone();
                    if add_node_to_db(db, self_addr.ip(), node, self_addr) {
                        print_node_value(self_addr.to_string(), value);
                    } else {
                        println!("Failed to update value");
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
        // If the requested connection is to a node that is on the black list,
        // do not attempt to connect
        let black_list = black_list.lock();
        if black_list.contains(&(ip, port)) {
            return;
        }
    }

    // Start a TCP connection to the requested node
    let mut socket = match TcpStream::connect((ip, port)).await {
        Ok(socket) => socket,
        Err(_) => {
            // If the connection failed, add the node to the black list
            // and remove from db if it was previously added
            blacklist_node(db, black_list, ip, port);
            return;
        }
    };

    // Read data from the socket into the buffer
    let mut buf: String = String::new();
    socket.read_to_string(&mut buf).await.unwrap();

    // Get rid of all whitespace except for newlines
    buf.retain(|c| !c.is_whitespace() || c == '\n');

    // Split on each new line
    let mut lines: Vec<&str> = buf.split('\n').collect();

    // Remove empty lines
    lines = lines.into_iter().filter(|line| !line.is_empty()).collect();

    let mut change_made = false;
    let mut line_counter = 0;

    for line in lines {
        // Split line on each comma, and remove empty elements
        let mut split_line: Vec<&str> = line.split(',').collect();
        split_line = split_line
            .into_iter()
            .filter(|element| !element.is_empty())
            .collect();

        // If first element of line is a valid socket address, set to peer_socket
        // Otherwise, skip to next line
        let peer_socket = match SocketAddrV4::from_str(split_line[0]) {
            Ok(p_socket) => p_socket,
            Err(_) => {
                continue;
            }
        };

        // Ignore the line if it refers to this node
        if peer_socket == self_addr {
            continue;
        }

        // Parse timestamp and if it is invalid, skip to next line
        let time = match split_line[1].parse::<u64>() {
            Ok(time) => SystemTime::UNIX_EPOCH + Duration::from_secs(time),
            Err(_) => {
                continue;
            }
        };

        // If the timestamp is in the future, skip to next line
        if SystemTime::now().duration_since(time).is_err() {
            continue;
        }

        // Parse value and if it is invalid, skip to next line
        let value = match split_line[2].parse::<u8>() {
            Ok(value) if value < 10 => value,
            _ => {
                continue;
            }
        };
        {
            let node = Node {
                port: peer_socket.port(),
                time,
                value,
            };

            let db = db.clone();
            if add_node_to_db(db, peer_socket.ip(), node, self_addr) {
                if !change_made {
                    println!();
                    change_made = true;
                }
                print_node_value(peer_socket.to_string(), value);
            }
        }

        // We only want to consider the first 256 lines received
        line_counter += 1;
        if line_counter >= 256 {
            break;
        }
    }
    // If a new node/value was processed and printed, print the prompt again
    if change_made {
        print!(">> ");
        std::io::stdout().flush().expect("Failed to flush stdout");
    }
}

fn print_node_value(node: String, value: u8) {
    println!("{} --> {}", node, value);
}

async fn handle_first_time_node_connection_req(
    db: Db,
    black_list: BlackList,
    self_addr: SocketAddrV4,
    mut rx: mpsc::Receiver<(Ipv4Addr, u16)>,
) {
    loop {
        while let Some(message) = rx.recv().await {
            let db = db.clone();
            let black_list = black_list.clone();
            poll_node(db, message.0, message.1, self_addr, black_list).await;
        }
    }
}

async fn background_node_polling(db: Db, black_list: BlackList, self_addr: SocketAddrV4) {
    loop {
        // Selected socket address to poll
        let mut maybe_socket: Option<SocketAddrV4> = None;
        {
            // Obtain a mutable copy of the database as a vector of tuples
            let db = db.lock();
            let db_vec: Vec<(&Ipv4Addr, &Vec<Node>)> = db.iter().collect();
            let mut node_vec: Vec<(&Ipv4Addr, Vec<Node>)> = db_vec
                .iter()
                .map(|(ip, nodes)| (*ip, nodes.clone().to_owned()))
                .collect();
            // Our "retry loop", in case our first attempt returns the current node
            // This should only run a maximum of 2 times
            while !node_vec.is_empty() {
                // Select a random IP address from the database
                let random_index = rand::random::<usize>() % node_vec.len();
                let (ip, nodes) = node_vec.remove(random_index);
                let mut nodes_copy = nodes.to_vec();

                // Select a random port from the selected IP address
                let random_index = rand::random::<usize>() % nodes.len();
                let node = nodes_copy.remove(random_index);

                // If the selected node is not the current node, set it as the selected socket
                // and break out of the loop
                if *ip != *self_addr.ip() || node.port != self_addr.port() {
                    maybe_socket = Some(SocketAddrV4::new(*ip, node.port));
                    break;
                } else if !nodes_copy.is_empty() {
                    // otherwise, if there are remaining ports for the selected IP address,
                    // add them back to the vector
                    node_vec.push((ip, nodes_copy.as_slice().to_vec()));
                }
            }
        }
        // If we successfully selected a socket, poll it
        if let Some(socket) = maybe_socket {
            let db = db.clone();
            let black_list = black_list.clone();
            poll_node(db, *socket.ip(), socket.port(), self_addr, black_list).await;
        }

        sleep(BACKGROUND_POLLING_INTERVAL).await;
    }
}

fn blacklist_node(db: Db, black_list: BlackList, ip: Ipv4Addr, port: u16) {
    let mut black_list = black_list.lock();
    black_list.push((ip, port));
    let mut db = db.lock();
    if let Some(nodes) = db.get_mut(&ip) {
        nodes.retain(|node| node.port != port);
    }
}

fn parse_ip_and_port_from_input(input: &str) -> Result<(Ipv4Addr, u16), Box<dyn Error>> {
    let split_input: Vec<&str> = input.split(":").collect();
    let ip: &str = &split_input[0]
        .trim()
        .to_string()
        .get(1..)
        .expect("Invalid socket address")
        .to_owned();

    let ip = Ipv4Addr::from_str(ip.trim())?;
    let port = split_input.get(1).unwrap_or(&"").trim().parse::<Port>()?;
    Ok((ip, port))
}

fn add_node_to_db(db: Db, ip: &Ipv4Addr, node: Node, self_addr: SocketAddrV4) -> bool {
    let mut db = db.lock();
    // Try to find the node in the db
    if let Some(nodes) = db.get_mut(ip) {
        for mut n in nodes.iter_mut() {
            // If we find the node and the timestamp is newer, update the node
            if n.port == node.port {
                if node.time.duration_since(n.time).is_ok() && node.value != n.value {
                    n.time = node.time;
                    n.value = node.value;

                    return true;
                } else {
                    return false;
                }
            }
        }

        // If we didn't find the node in the db, add it to the db
        // if the ip address isn't full or it's timestamp is new enough
        nodes.push(node.clone());

        // If the ip address is full, keep the newest 3 updated nodes
        // and remove the rest
        if nodes.len() > 3 {
            nodes.sort_by(|a, b| {
                if *ip == *self_addr.ip() {
                    if a.port == self_addr.port() {
                        Ordering::Greater
                    } else if b.port == self_addr.port() {
                        Ordering::Less
                    } else {
                        a.time.cmp(&b.time)
                    }
                } else {
                    a.time.cmp(&b.time)
                }
            });
            nodes.reverse();
            nodes.truncate(3);
        }

        if nodes.contains(&node) {
            return true;
        } else {
            return false;
        }
    } else {
        // If no nodes were found at the ip address, add the node to the db
        db.insert(*ip, vec![node]);

        return true;
    }
}
