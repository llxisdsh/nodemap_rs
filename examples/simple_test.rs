use nodemap_rs::NodeMap;

fn main() {
    println!("Creating NodeMap...");
    let map = NodeMap::new();

    println!("Inserting values...");
    map.insert(1, 10);
    map.insert(2, 20);

    println!("Testing for_each...");

    for (k, v) in map.iter() {
        println!("  {} -> {}", k, v);
    }

    println!("Done!");
}
