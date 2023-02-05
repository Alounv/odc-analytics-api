use bson::{doc, oid::ObjectId, Bson, Document};
use http::StatusCode;
use mongodb::{
    options::{ClientOptions, ResolverConfig},
    Client,
};
use std::env;
use std::error::Error;
use std::{collections::HashMap, str::FromStr};
use url::Url;
use vercel_lambda::{error::VercelError, lambda, IntoResponse, Request, Response};

fn parse_url(req: &Request) -> Result<(String, String), Box<dyn Error>> {
    let parsed_url = Url::parse(&req.uri().to_string()).unwrap();
    let hash_query: HashMap<_, _> = parsed_url.query_pairs().into_owned().collect();
    if hash_query.contains_key("db") && hash_query.contains_key("publication") {
        let db_name = hash_query.get("db").unwrap().to_string();
        let publication_id = hash_query.get("publication").unwrap().to_string();
        Ok((db_name, publication_id))
    } else {
        Err("Missing db or publication parameters".into())
    }
}

#[tokio::main]
async fn list_db(
    db_name: &str,
    publication_id: ObjectId,
) -> Result<Option<bson::Document>, mongodb::error::Error> {
    let uri = env::var("MONGODB_URI").expect("MONGODB_URI must be set");
    let options =
        ClientOptions::parse_with_resolver_config(&uri, ResolverConfig::cloudflare()).await?;
    let client = Client::with_options(options)?;
    println!("Connected to MongoDB!");

    let db = client.database(db_name);
    let publications = db.collection::<Document>("course");

    let publication = publications
        .find_one(doc! { "_id": publication_id }, None)
        .await;

    publication

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

fn get_results(req: Request) -> Result<Bson, Box<dyn Error>> {
    let (db_name, publication_id) = parse_url(&req)?;
    let publication_id = ObjectId::from_str(&publication_id)?;
    match list_db(&db_name, publication_id) {
        Ok(results) => match results {
            Some(doc) => Ok(Bson::Document(doc)),
            None => Ok(Bson::Null),
        },
        Err(e) => Err(e.into()),
    }
}

fn handler(req: Request) -> Result<impl IntoResponse, VercelError> {
    let result = get_results(req);

    match result {
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
    use vercel_lambda::Body;

    use super::*;

    #[test]
    fn test_get_results() {
        env::set_var("MONGODB_URI", "mongodb://localhost:27017");
        let req = http::Request::builder()
            .uri("https://api.example.com/analytics?db=V5&publication=60dc4225f9f392004ebfb7fd")
            .body(Body::Empty)
            .unwrap();
        let result = get_results(req).unwrap();
        assert!(result
            .to_string()
            .contains("\"_id\": ObjectId(\"60dc4225f9f392004ebfb7fd\")"));
    }
}
