use http::StatusCode;
use mongodb::{
    options::{ClientOptions, ResolverConfig},
    Client,
};
use std::env;
use std::error::Error;
use vercel_lambda::{error::VercelError, lambda, IntoResponse, Request, Response};

#[tokio::main]
pub async fn list_db() -> Result<Vec<String>, mongodb::error::Error> {
    let uri = env::var("MONGODB_URI").expect("MONGODB_URI must be set");
    let options = ClientOptions::parse_with_resolver_config(&uri, ResolverConfig::cloudflare())
        .await
        .expect("Failed to parse options");
    let client = Client::with_options(options).expect("Failed to initialize client");

    return client.list_database_names(None, None).await;
}

fn handler(_req: Request) -> Result<impl IntoResponse, VercelError> {
    match list_db() {
        Ok(list) => {
            let data = serde_json::json!(list);
            let response = Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "text/plain")
                .body(data.to_string())
                .expect("Internal Server Error");

            Ok(response)
        }
        Err(e) => {
            let error = format!("Error: {}", e);
            Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(error)
                .unwrap())
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    Ok(lambda!(handler))
}

#[cfg(test)]
mod tests {
    // use super::*;

    #[test]
    fn test_example() {
        assert_eq!(true, true);
    }
}
