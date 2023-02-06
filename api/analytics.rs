use bson::{doc, oid::ObjectId, Document};
use chrono::Utc;
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

async fn get_superadmin_ids(db: &Database) -> Result<Vec<ObjectId>, Box<dyn Error>> {
    let collection = db.collection::<Document>("user");
    let superadmins = collection
        .distinct("_id", doc! {"roles": "superadmin"}, None)
        .await?;

    let superadmins = superadmins
        .iter()
        .map(|doc| doc.as_object_id().unwrap().clone())
        .collect();
    Ok(superadmins)
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

async fn get_active_modules(
    db: &Database,
    publication_id: &str,
) -> Result<Vec<ObjectId>, Box<dyn Error>> {
    let collection = db.collection::<Document>("course");
    let publication_id = ObjectId::from_str(publication_id)?;

    let cursor = collection
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
                    "$project": doc! { "_id": "$modules._id" }
                },
            ],
            None,
        )
        .await?;

    let active_modules = cursor.try_collect::<Vec<Document>>().await?;
    let active_modules = active_modules
        .iter()
        .map(|doc| doc.get_object_id("_id").unwrap().clone())
        .collect();
    Ok(active_modules)
}

#[derive(Debug, Serialize, Deserialize)]
struct CompletionDate {
    user: ObjectId,
    #[serde(with = "bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    date: chrono::DateTime<Utc>,
}

async fn get_completion_dates(
    db: &Database,
    publication_id: &str,
    active_modules: &Vec<ObjectId>,
) -> Result<Vec<CompletionDate>, Box<dyn Error>> {
    let collection = db.collection::<Document>("course");
    let publication_id = ObjectId::from_str(publication_id)?;

    let cursor = collection
        .aggregate(
            [
                doc! {
                    "$match": doc! {
                        "context.course": publication_id,
                        "courseModule": doc! {
                            "$in":  active_modules,
                        },
                        "user": doc! {
                            "$exists": true
                        },
                        "isClear": true
                    }
                },
                doc! {
                    "$group": doc! {
                        "_id": doc! {
                            "user": "$user",
                            "module": "$courseModule"
                        },
                        "earliestCompletion": doc! {
                            "$min": "$date_updated"
                        }
                    }
                },
                doc! {
                    "$group": doc! {
                        "_id": "$_id.user",
                        "courseCompletionDate": doc! {
                            "$max": "$earliestCompletion"
                        },
                        "completedModulesCount": doc! {
                            "$sum": 1
                        }
                    }
                },
                doc! {
                    "$match": doc! {
                        "completedModulesCount": active_modules.len() as u32
                    }
                },
                doc! {
                    "$project": doc! {
                        "_id": 0,
                        "user": "$_id",
                        "date": "$courseCompletionDate"
                    }
                },
            ],
            None,
        )
        .await?;

    let completion_dates = cursor.try_collect::<Vec<Document>>().await?;
    let completion_dates = completion_dates
        .iter()
        .map(|doc| bson::from_document::<CompletionDate>(doc.clone()).unwrap())
        .collect();
    Ok(completion_dates)
}

#[derive(Debug, Serialize, Deserialize)]
struct GranuleSprint {
    course_module_id: ObjectId,
    sprint_ids: Vec<ObjectId>,
}

#[derive(Debug, Serialize, Deserialize)]
struct UserSessionSprint {
    user: ObjectId,
    completed_modules_count: u32,
    completion_percentage: f32,
    completed_modules_ids: Vec<ObjectId>,
    started_modules_ids: Vec<ObjectId>,
    #[serde(with = "bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    date_started: chrono::DateTime<Utc>,
    #[serde(with = "bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    last_activity: chrono::DateTime<Utc>,
    granule_sprints: Vec<GranuleSprint>,
}

async fn get_users_session_sprints(
    db: &Database,
    publication_id: &str,
    active_modules: &Vec<ObjectId>,
) -> Result<Vec<UserSessionSprint>, Box<dyn Error>> {
    let collection = db.collection::<Document>("course_module_sprint");

    let cursor = collection
        .aggregate(
            [
                doc! {
                    "$match": doc! {
                        "_cls": "Training",
                        "context.course": ObjectId::parse_str(publication_id)?,
                        "user": doc! {
                            "$exists": true
                        }
                    }
                },
                doc! {
                    "$unwind": "$user"
                },
                doc! {
                    "$unwind": "$granuleSprints"
                },
                doc! {
                    "$group": doc! {
                        "_id": doc! {
                            "user": "$user",
                            "courseModule": "$courseModule"
                        },
                        "course": doc! {
                            "$first": "$context.course"
                        },
                        "dateStarted": doc! {
                            "$min": "$date_created"
                        },
                        "lastActivity": doc! {
                            "$max": "$date_updated"
                        },
                        "granuleSprints": doc! {
                            "$addToSet": "$granuleSprints"
                        },
                        "isClear": doc! {
                            "$max": "$isClear"
                        },
                        "courseModule": doc! {
                            "$first": "$courseModule"
                        }
                    }
                },
                doc! {
                    "$set": doc! {
                        "activeModulesIds": active_modules,
                    }
                },
                doc! {
                    "$group": doc! {
                        "_id": "$_id.user",
                        "course": doc! {
                            "$first": "$course"
                        },
                        "dateStarted": doc! {
                            "$min": "$dateStarted"
                        },
                        "lastActivity": doc! {
                            "$max": "$lastActivity"
                        },
                        "granuleSprints": doc! {
                            "$push": doc! {
                                "course_module_id": "$_id.courseModule",
                                "sprint_ids": "$granuleSprints"
                            }
                        },
                        "completedModulesIds": doc! {
                            "$addToSet": doc! {
                                "$cond": [
                                    doc! {
                                        "$and": [
                                            "$isClear",
                                            doc! {
                                                "$in": [
                                                    "$courseModule",
                                                    "$activeModulesIds"
                                                ]
                                            }
                                        ]
                                    },
                                    "$courseModule",
                                    "$$REMOVE"
                                ]
                            }
                        },
                        "startedModulesIds": doc! {
                            "$addToSet": doc! {
                                "$cond": [
                                    doc! {
                                        "$in": [
                                            "$courseModule",
                                            "$activeModulesIds"
                                        ]
                                    },
                                    "$courseModule",
                                    "$$REMOVE"
                                ]
                            }
                        }
                    }
                },
                doc! {
                    "$project": doc! {
                        "_id": 0,
                        "user": "$_id",
                        "date_started" : "$dateStarted",
                        "last_activity": "$lastActivity",
                        "granule_sprints": "$granuleSprints",
                        "completed_modules_count": doc! {
                            "$size": "$completedModulesIds"
                        },
                        "completion_percentage": doc! {
                            "$round": doc! {
                                "$multiply": [
                                    doc! {
                                        "$divide": [
                                            doc! {
                                                "$size": "$completedModulesIds"
                                            },
                                            active_modules.len() as u32
                                        ]
                                    },
                                    100
                                ]
                            }
                        },
                        "completed_modules_ids": "$completedModulesIds",
                        "started_modules_ids": "$startedModulesIds"
                    }
                },
            ],
            None,
        )
        .await?;

    let users_session_sprints = cursor.try_collect::<Vec<Document>>().await?;
    let users_session_sprints = users_session_sprints
        .iter()
        .map(|doc| bson::from_document::<UserSessionSprint>(doc.clone()).unwrap())
        .collect();
    Ok(users_session_sprints)
}

#[tokio::main]
async fn calc(db_name: &str, publication_id: &str) -> Result<Vec<Document>, Box<dyn Error>> {
    // The array at the beginning of the line indicates what is required before the calculation (u
    // is for user and c for course)

    let start = Instant::now();

    let db = gt_db(db_name).await?;
    println!(
        "⏱️ {} ms - connection to MongoDB",
        start.elapsed().as_millis()
    );

    let (users_groups_names, active_modules, superadmins) = tokio::try_join!(
        get_users_groups_names(&db, publication_id), // TODO: add possiblity to filter for one user
        get_active_modules(&db, publication_id),
        get_superadmin_ids(&db),
    )?;
    println!(
        "⏱️ {} ms - users_groups_names {}, superadmins {} and active_modules {}",
        start.elapsed().as_millis(),
        users_groups_names.len(),
        superadmins.len(),
        active_modules.len()
    );

    let (users_session_sprints, completion_dates) = tokio::try_join!(
        get_users_session_sprints(&db, &publication_id, &active_modules),
        get_completion_dates(&db, &publication_id, &active_modules),
    )?;
    // (This should include the completion date at the end of the aggregate).
    println!(
        "⏱️ {} ms - users_session_sprints {}, completion_dates {}",
        start.elapsed().as_millis(),
        users_session_sprints.len(),
        completion_dates.len()
    );

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

        let result_option = get_results(req);
        assert!(result_option.is_ok());

        let result = result_option.unwrap();
        println!("result: {:#?}", result.len());
        assert!(true)
    }
}
