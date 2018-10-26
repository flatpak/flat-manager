extern crate jsonwebtoken as jwt;
#[macro_use]
extern crate serde_derive;
extern crate base64;
extern crate chrono;

use std::env;
use std::io;
use jwt::{encode, Header};
use chrono::{Utc, Duration};

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String,
    scope: Vec<String>,
    name: String,
    exp: i64,
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        println!("Invalid args, must give name!");
    }

    let mut input = String::new();

    io::stdin().read_line(&mut input).unwrap();

    let bytes = base64::decode(input.trim()).unwrap();

    let claims = Claims {
        sub: "build".to_string(),
        scope: vec!["build".to_string(), "upload".to_string(), "publish".to_string()],
        name: args[1].clone(),
        exp: Utc::now().timestamp() + Duration::days(365).num_seconds(),
    };

    let token = encode(&Header::default(), &claims, &bytes).unwrap();
    println!("{}", token);
}
