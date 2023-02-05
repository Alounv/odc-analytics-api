use bson::{doc, oid::ObjectId, Bson, Document};
use futures::stream::TryStreamExt;
use http::StatusCode;
use mongodb::{
    options::{ClientOptions, ResolverConfig},
    Client, Database,
};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::{collections::HashMap, str::FromStr};
use std::{env, time::Instant};
use url::Url;
use vercel_lambda::{error::VercelError, lambda, IntoResponse, Request, Response};

fn parse_url(req: &Request) -> Result<(String, String), Box<dyn Error>> {
    let parsed_url = Url::parse(&req.uri().to_string())?;
    let hash_query: HashMap<_, _> = parsed_url.query_pairs().into_owned().collect();
    if hash_query.contains_key("db") && hash_query.contains_key("publication") {
        let db_name = hash_query.get("db").unwrap().to_string();
        let publication_id = hash_query.get("publication").unwrap().to_string();
        Ok((db_name, publication_id))
    } else {
        Err("Missing db or publication parameters".into())
    }
}

async fn get_client() -> Result<Client, Box<dyn Error>> {
    let uri = env::var("MONGODB_URI").expect("MONGODB_URI must be set");
    let options =
        ClientOptions::parse_with_resolver_config(&uri, ResolverConfig::cloudflare()).await?;
    let client = Client::with_options(options)?;
    Ok(client)
}

async fn gt_db(db_name: &str) -> Result<Database, Box<dyn Error>> {
    let client = get_client().await?;
    let db = client.database(db_name);
    Ok(db)
}

#[derive(Debug, Serialize, Deserialize)]
struct ActiveModules {
    active_modules: Vec<ObjectId>,
}

async fn get_active_modules(
    db: &Database,
    publication_id: &str,
) -> Result<Vec<ObjectId>, Box<dyn Error>> {
    let collection = db.collection::<Document>("course");
    let publication_id = ObjectId::from_str(publication_id)?;

    let mut cursor = collection
        .aggregate(
            [
                doc! { "$match": doc! { "_id": publication_id, } },
                doc! { "$project": doc! { "chapters.modules": 1 } },
                doc! {
                    "$lookup": doc! {
                        "from": "course_module",
                        "localField": "chapters.modules",
                        "foreignField": "_id",
                        "as": "modules"
                    }
                },
                doc! {
                    "$unwind": doc! {
                        "path": "$modules",
                        "preserveNullAndEmptyArrays": true
                    }
                },
                doc! {
                    "$lookup": doc! {
                        "from": "topic",
                        "localField": "modules.topic",
                        "foreignField": "_id",
                        "as": "modules.topic"
                    }
                },
                doc! {
                    "$lookup": doc! {
                        "from": "granule",
                        "localField": "modules.topic.granules",
                        "foreignField": "_id",
                        "as": "modules.topic.granules"
                    }
                },
                doc! {
                    "$match": doc! {
                        "modules.topic.granules": doc! {
                            "$elemMatch": doc! { "isDraft": doc! { "$ne": true } }
                        }
                    }
                },
                doc! {
                    "$group": doc! {
                        "_id": Bson::Null,
                        "active_modules": doc! {
                            "$push": "$modules._id"
                        }
                    }
                },
            ],
            None,
        )
        .await?;

    if let Some(document) = cursor.try_next().await? {
        let publication: ActiveModules = bson::from_document(document)?;
        Ok(publication.active_modules)
    } else {
        Err("Publication not found".into())
    }
}

async fn get_users_groups_names(
    db: &Database,
    publication_id: &str,
) -> Result<Vec<Document>, Box<dyn Error>> {
    let groups = db.collection::<Document>("learners_group");
    let publication_id = ObjectId::from_str(publication_id)?;
    let cursor = groups
        .aggregate(
            [
                doc! { "$match": { "publications": publication_id }, },
                doc! { "$unwind": "$users", },
                doc! {
                  "$group": {
                    "_id": "$users",
                    "groups": { "$push": "$name", },
                  },
                },
                doc! {
                    "$project": {
                        "user": "$_id",
                        "groups": 1,
                    },
                },
            ],
            None,
        )
        .await?;
    let users_groups_names = cursor.try_collect::<Vec<Document>>().await?;
    Ok(users_groups_names)
}

#[tokio::main]
async fn calc(db_name: &str, publication_id: &str) -> Result<Vec<Document>, Box<dyn Error>> {
    // The array at the beginning of the line indicates what is required before the calculation (u
    // is for user and c for course)

    let start = Instant::now();

    let db = gt_db(db_name).await?;
    println!("Connection to MongoDB: {} ms", start.elapsed().as_millis());

    // BATCH A
    // 1. [u?] GET the usersGroupsNames PER USER  ---> 'learners_group' collection
    // 2. [c] GET the activeModules for the publication ---> 'course' collection
    let (users_groups_names, active_modules) = tokio::try_join!(
        get_users_groups_names(&db, publication_id), // TODO: add possiblity to filter for one user
        get_active_modules(&db, publication_id),
    )?;
    println!(
        "Get users_groups_names and active_modules: {} ms",
        start.elapsed().as_millis()
    );

    println!("active_modules: {:#?}", active_modules);

    Ok(users_groups_names)

    /*
     * BATCH B
     * 4. [2, u?] -> GET the usersSessionSprints for the course PER USER ---> 'course_module_sprint' collection
     * (This should include the completion date at the end of the aggregate).
     *
     * BATCH C
     * 5. [4] -> group granuleSprints PER USE (1st level) and PER MODULE (2nd level)
     *
     * BATCH D
     * 6. [5] -> GET usersModulesDurations PER USER (1st level) and PER MODULE (2nd level) ---> 'granule_sprint' collection
     *
     * BATCH E
     * 8. [2,4,5] -> loop through each usersSessionsSprints
     *    8.1. Format course data
     *    8.2. Format module specific data
     */
}

fn get_results(req: Request) -> Result<Vec<Document>, Box<dyn Error>> {
    let (db_name, publication_id) = parse_url(&req)?;
    match calc(&db_name, &publication_id) {
        Ok(results) => Ok(results),
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
        println!("{:?}", result.len());
        assert!(true)
    }
}
