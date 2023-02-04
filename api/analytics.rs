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

    // TODO pass db name + course id as parameter
    // TODO print duration of each operation

    /*
     * The array at the beginning of the line indicates what is required before the calculation (u
     * is for user and c for course)
     *
     * BATCH A
     * 1. [u?] GET the usersGroupsNames PER USER  ---> 'learners_group' collection
     * 2. [c] GET the publication for one database (name in the query) ---> 'course' collection
     *
     * BATCH B
     * 3. [2] -> calculate the activeModulesIds
     * 4. [2, u?] -> GET the usersSessionSprints for the course PER USER ---> 'course_module_sprint' collection
     * (This should include the completion date at the end of the aggregate).
     *
     * BATCH C
     * 5. [4] -> group granuleSprints PER USE (1st level) and PER MODULE (2nd level)
     * 6. [5] -> GET usersModulesDurations PER USER (1st level) and PER MODULE (2nd level) ---> 'granule_sprint' collection
     *
     * BATCH D
     * 8. [3,4,5] -> loop through each usersSessionsSprints
     *    8.1. Format course data
     *    8.2. Format module specific data
     */
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
