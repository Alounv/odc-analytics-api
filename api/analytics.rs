use bson::{doc, oid::ObjectId, Bson::Null, Document};
use chrono::{DateTime, Utc};
use flate2::write::GzEncoder;
use flate2::Compression;
use futures::stream::TryStreamExt;
use http::{Response, StatusCode};
use mongodb::{
    options::{ClientOptions, ResolverConfig},
    Client, Database,
};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::io::prelude::*;
use std::{collections::HashMap, str::FromStr};
use std::{env, time::Instant};
use tokio::task;
use url::Url;
use vercel_lambda::{error::VercelError, lambda, Body, IntoResponse, Request};

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

#[derive(Debug, Serialize, Deserialize, Clone)]
struct User {
    _id: ObjectId,
    email: String,
    lms_learner_id: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct DbUser {
    _id: ObjectId,
    email: String,
    lms_learner_id: Option<String>,
}

async fn get_users(db: &Database) -> Result<HashMap<ObjectId, User>, Box<dyn Error>> {
    let start = Instant::now();
    let collection = db.collection::<Document>("user");
    let cursor = collection
        .aggregate(
            [
                doc! {"$match": doc! {"roles": doc! { "$ne": "superadmin" }} },
                doc! {"$project": doc! {"_id": 1, "email": 1, "lms_learner_id": "$lmsLearnerId"}},
            ],
            None,
        )
        .await?;

    let documents = cursor.try_collect::<Vec<Document>>().await?;
    let users = documents
        .iter()
        .map(|doc| {
            let value = bson::from_document::<DbUser>(doc.clone()).unwrap();
            (
                value._id,
                User {
                    _id: value._id,
                    email: value.email,
                    lms_learner_id: value.lms_learner_id.unwrap_or("".to_string()),
                },
            )
        })
        .collect();

    println!("  ➡️ get_users: {} ms", start.elapsed().as_millis());
    Ok(users)
}

#[derive(Debug, Serialize, Deserialize)]
struct UserGroupsNames {
    user: ObjectId,
    groups: Vec<String>,
}

async fn get_users_groups_names(
    db: &Database,
    publication_id: &str,
) -> Result<HashMap<ObjectId, UserGroupsNames>, Box<dyn Error>> {
    let start = Instant::now();
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
                        "_id": 0,
                        "user": "$_id",
                        "groups": 1,
                    },
                },
            ],
            None,
        )
        .await?;
    let documents = cursor.try_collect::<Vec<Document>>().await?;
    let users_groups_names = documents
        .iter()
        .map(|doc| {
            let value = bson::from_document::<UserGroupsNames>(doc.clone()).unwrap();
            (value.user, value)
        })
        .collect();

    println!(
        "  ➡️ get_users_groups_names: {:?} ms",
        start.elapsed().as_millis()
    );
    Ok(users_groups_names)
}

#[derive(Debug, Serialize, Deserialize)]
struct ActiveModule {
    module: ObjectId,
    active_granules: Vec<ObjectId>,
}

async fn get_active_modules(
    db: &Database,
    publication_id: &str,
) -> Result<Vec<ActiveModule>, Box<dyn Error>> {
    let start = Instant::now();
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
                    "$project": doc! {
                        "_id": 0,
                        "module": "$modules._id",
                        "active_granules": doc! {
                            "$filter": doc! {
                                "input": "$modules.topic.granules",
                                "as": "granule",
                                "cond": doc! { "$ne": [ "$$granule.isDraft", true ] }
                            },
                        },
                    }
                },
                doc! {
                    "$set": doc! {
                        "active_granules": doc! {
                            "$map": doc! {
                                "input": "$active_granules",
                                "as": "granule",
                                "in": "$$granule._id"
                            }
                        }
                    }
                },
            ],
            None,
        )
        .await?;

    let documents = cursor.try_collect::<Vec<Document>>().await?;
    let active_modules = documents
        .iter()
        .map(|doc| bson::from_document::<ActiveModule>(doc.clone()).unwrap())
        .collect();

    println!("  ➡️ get_active_modules: {} ms", start.elapsed().as_millis());
    Ok(active_modules)
}

#[derive(Debug, Serialize, Deserialize)]
struct CompletionDate {
    user: ObjectId,
    #[serde(with = "bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    date: DateTime<Utc>,
}

async fn get_completion_dates(
    db: &Database,
    publication_id: &str,
    active_modules_id: &Vec<ObjectId>,
) -> Result<HashMap<ObjectId, CompletionDate>, Box<dyn Error>> {
    let start = Instant::now();

    let collection = db.collection::<Document>("course_module_sprint");
    let publication_id = ObjectId::from_str(publication_id)?;

    let cursor = collection
        .aggregate(
            [
                doc! {
                    "$match": doc! {
                        "context.course": publication_id,
                        "courseModule": doc! { "$in":  active_modules_id, },
                        "user": doc! { "$ne": Null },
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
                        "completedModulesCount": active_modules_id.len() as u32
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

    let documents = cursor.try_collect::<Vec<Document>>().await?;
    let completion_dates = documents
        .iter()
        .map(|doc| {
            let value = bson::from_document::<CompletionDate>(doc.clone()).unwrap();
            (value.user, value)
        })
        .collect();

    println!(
        "  ➡️ get_completion_dates: {} ms",
        start.elapsed().as_millis()
    );
    Ok(completion_dates)
}

#[derive(Debug, Serialize, Deserialize)]
struct UserSessionSprint {
    user: ObjectId,
    completed_modules_count: u32,
    completion_percentage: f32,
    completed_modules_ids: Vec<ObjectId>,
    started_modules_ids: Vec<ObjectId>,
    #[serde(with = "bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    date_started: DateTime<Utc>,
    #[serde(with = "bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    last_activity: DateTime<Utc>,
}

async fn get_users_session_sprints(
    db: &Database,
    publication_id: &str,
    active_modules_id: &Vec<ObjectId>,
) -> Result<HashMap<ObjectId, UserSessionSprint>, Box<dyn Error>> {
    let start = Instant::now();
    let collection = db.collection::<Document>("course_module_sprint");
    let publication_id = ObjectId::from_str(publication_id)?;

    let cursor = collection
        .aggregate(
            [
                doc! {
                    "$match": doc! {
                        "_cls": "Training",
                        "context.course": publication_id,
                        "courseModule": doc! { "$in":  active_modules_id, },
                        "user": doc! { "$ne": Null, },
                    }
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
                        "isClear": doc! {
                            "$max": "$isClear"
                        },
                        "courseModule": doc! {
                            "$first": "$courseModule"
                        }
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
                        "completedModulesIds": doc! {
                            "$addToSet": doc! {
                                "$cond": [
                                    "$isClear",
                                    "$courseModule",
                                    "$$REMOVE"
                                ]
                            }
                        },
                        "startedModulesIds": doc! {
                            "$addToSet": "$courseModule",
                        }
                    }
                },
                doc! {
                    "$project": doc! {
                        "_id": 0,
                        "user": "$_id",
                        "date_started" : "$dateStarted",
                        "last_activity": "$lastActivity",
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
                                            active_modules_id.len() as u32
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
                doc! {
                    "$match": doc! {
                        "user": doc! { "$ne": Null, },
                    }
                },
            ],
            None,
        )
        .await?;

    let documents = cursor.try_collect::<Vec<Document>>().await?;
    let users_session_sprints = documents
        .iter()
        .map(|doc| {
            let value = bson::from_document::<UserSessionSprint>(doc.clone()).unwrap();
            (value.user, value)
        })
        .collect::<HashMap<ObjectId, UserSessionSprint>>();

    println!(
        "  ➡️ get_users_session_sprints: {} ms",
        start.elapsed().as_millis()
    );
    Ok(users_session_sprints)
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct UserModuleDuration {
    module: ObjectId,
    ms_duration: i64,
    formatted_duration: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct UserModulesDurations {
    user: ObjectId,
    modules_durations: Vec<UserModuleDuration>,
}

async fn get_users_modules_durations(
    db: &Database,
    publication_id: &str,
    active_modules: &Vec<ActiveModule>,
) -> Result<HashMap<ObjectId, Vec<UserModuleDuration>>, Box<dyn Error>> {
    let start = Instant::now();
    let publication_id = ObjectId::parse_str(publication_id)?;
    let mut users_modules_durations = Vec::new();

    for module in active_modules {
        let db_ref = db.clone();
        let module_id = module.module.clone();
        let module_active_granules = module.active_granules.clone();
        let collection = db_ref.collection::<Document>("course_module_sprint");

        let users_module_durations = task::spawn(async move {
            let cursor = collection
            .aggregate(
                [
                    doc! {
                        "$match": doc! {
                            "_cls": "Training",
                            "context.course": publication_id,
                            "courseModule": module_id,
                            "user": doc! { "$ne": Null },
                        }
                    },
                    doc!{
                        "$lookup": doc! {
                            "from": "granule_sprint",
                            "localField": "granuleSprints",
                            "foreignField": "_id",
                            "as": "granuleSprints"
                        }
                    },
                    doc! {
                        "$unwind": "$granuleSprints"
                    },
                    doc! {
                        "$match": doc! {
                            "granuleSprints.granule": doc! { "$in": module_active_granules }
                        }
                    },
                    doc! {
                        "$set": {
                            "granuleSprints.courseModule": "$courseModule"
                        }
                    },
                    doc! {
                        "$replaceRoot": doc! {
                            "newRoot": "$granuleSprints"
                        }
                    },
                    doc! {
                        "$set": doc! {
                            "msSessionDuration": doc! {
                                "$subtract": [
                                    doc! { "$arrayElemAt": [ "$events.timestamp", -1 ] },
                                    doc! { "$arrayElemAt": [ "$events.timestamp", 0 ] }
                                ]
                            },
                            "isCompleted": doc! {
                                "$cond": [
                                    doc! { "$eq": [ "$isCompleted", true ] },
                                    1,
                                    0
                                ]
                            },
                            "breaks": doc! {
                                "$reduce": doc! {
                                    "input": doc! {
                                        "$filter": doc! {
                                            "input": "$events",
                                            "cond": doc! {
                                                "$or": [
                                                    doc! { "$eq": [ "$$this.eventType", "granuleReturn" ] },
                                                    doc! { "$and": [
                                                        doc! { "$eq": [ "$$this.eventType", "granuleLeft" ] },
                                                        doc! { "$ne": [ "$$this._id", doc! { "$arrayElemAt": [ "$events._id", -1 ] } ] }
                                                        ]
                                                    }
                                                ]
                                            }
                                        }
                                    },
                                    "initialValue": 0,
                                    "in": doc! {
                                        "$cond": [
                                            doc! {
                                                "$eq": [
                                                    "$$this.eventType",
                                                    "granuleLeft"
                                                ]
                                            },
                                            doc! {
                                                "$sum": [
                                                    "$$value",
                                                    doc! { "$toLong": "$$this.timestamp" }
                                                ]
                                            },
                                            doc! {
                                                "$subtract": [
                                                    "$$value",
                                                    doc! { "$toLong": "$$this.timestamp" }
                                                ]
                                            }
                                        ]
                                    }
                                }
                            }
                        }
                    },
                    doc! {
                        "$group": doc! {
                            "_id": doc! { "user": "$user", "module": "$courseModule" },
                            "msSessionDuration": doc! {
                                "$sum": "$msSessionDuration"
                            },
                            "breaks": doc! {
                                "$sum": "$breaks"
                            }
                        }
                    },
                    doc! {
                        "$project": doc! {
                            "msSessionDuration": doc! {
                                "$add": [
                                    "$msSessionDuration",
                                    "$breaks"
                                ]
                            }
                        }
                    },
                    doc! {
                        "$set": doc! {
                            "s": doc! {
                                "$toString": doc! {
                                    "$floor": doc! {
                                        "$mod": [ doc! { "$divide": [ "$msSessionDuration", 1000 ] }, 60 ]
                                    }
                                }
                            },
                            "m": doc! {
                                "$toString": doc! {
                                    "$floor": doc! {
                                        "$mod": [
                                            doc! { "$divide": [ "$msSessionDuration", doc! { "$multiply": 60000 } ] }, 60
                                        ]
                                    }
                                }
                            },
                            "h": doc! {
                                "$toString": doc! {
                                    "$floor": doc! {
                                        "$mod": [
                                            doc! { "$divide": [ "$msSessionDuration", doc! { "$multiply": 3600000 } ] }, 24
                                        ]
                                    }
                                }
                            }
                        }
                    },
                    doc! {
                        "$project": doc! {
                            "_id": 1,
                            "ms_duration": "$msSessionDuration",
                            "formatted_duration": doc! {
                                "$concat": [
                                    doc! {
                                        "$cond": [
                                            doc! { "$eq": [ doc! { "$strLenCP": "$h" }, 1 ] },
                                            doc! { "$concat": [ "0", "$h" ] },
                                            "$h"
                                        ]
                                    },
                                    ":",
                                    doc! {
                                        "$cond": [
                                            doc! { "$eq": [ doc! { "$strLenCP": "$m" }, 1 ] },
                                            doc! { "$concat": [ "0", "$m" ] },
                                            "$m"
                                        ]
                                    },
                                    ":",
                                    doc! {
                                        "$cond": [
                                            doc! { "$eq": [ doc! { "$strLenCP": "$s" }, 1 ] },
                                            doc! { "$concat": [ "0", "$s" ] },
                                            "$s"
                                        ]
                                    }
                                ]
                            }
                        }
                    },
                    doc! {
                        "$group": doc! {
                            "_id": "$_id.user",
                            "modules_durations": doc! {
                                "$push": doc! {
                                    "module": "$_id.module",
                                    "ms_duration": "$ms_duration",
                                    "formatted_duration": "$formatted_duration"
                                }
                            }
                        }
                    },
                    doc! {
                        "$project": doc! {
                            "_id": 0,
                            "user": "$_id",
                            "modules_durations": 1
                        }
                    },
                    doc! {
                        "$match": doc! {
                            "user": doc! { "$ne": Null }
                        }
                    }
                ],
                None,
            )
            .await.unwrap();

            let documents = cursor.try_collect::<Vec<Document>>().await.unwrap();
            let users_module_durations = documents
                .iter()
                .map(|doc| {
                    let value = bson::from_document::<UserModulesDurations>(doc.clone()).unwrap();
                    value
                })
                .collect::<Vec<UserModulesDurations>>();
                return users_module_durations;
        }).await?;

        users_modules_durations.extend(users_module_durations);
    }

    let users_modules_durations = users_modules_durations
        .iter()
        .map(|value| (value.user, value.modules_durations.clone()))
        .collect::<HashMap<ObjectId, Vec<UserModuleDuration>>>();
    println!(
        "  ➡️ get_users_modules_durations: {} ms",
        start.elapsed().as_millis()
    );
    Ok(users_modules_durations)
}

fn format_duration(duration: i64) -> String {
    let seconds = duration / 1000;
    let hours = seconds / 3600;
    let minutes = (seconds % 3600) / 60;
    let seconds = seconds % 60;
    format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
}

fn get_users_durations(
    user_modules_durations: Option<&Vec<UserModuleDuration>>,
) -> (String, Vec<UserModuleDuration>) {
    let mut result = ("N/A".to_string(), Vec::new());

    if let Some(user_durations) = user_modules_durations {
        let ms_duration = user_durations
            .iter()
            .map(|module| module.ms_duration)
            .sum::<i64>();
        result = (format_duration(ms_duration), user_durations.clone())
    }

    result
}

fn get_user_modules_analytics(
    active_modules: &Vec<ActiveModule>,
    completed_modules_ids: &Vec<ObjectId>,
    started_modules_ids: &Vec<ObjectId>,
    user_modules_durations: &Vec<UserModuleDuration>,
) -> Vec<UserModuleProgress> {
    active_modules
        .iter()
        .map(|active_module| {
            let module_id = active_module.module.clone();
            let mut progress = ModuleProgress::NotStarted;
            let is_completed = completed_modules_ids.contains(&module_id);
            if is_completed {
                progress = ModuleProgress::Completed;
            } else {
                let is_started = started_modules_ids.contains(&module_id);
                if is_started {
                    progress = ModuleProgress::Started;
                }
            }

            let duration = user_modules_durations
                .iter()
                .find(|user_module_duration| user_module_duration.module == module_id)
                .map(|user_module_duration| user_module_duration.formatted_duration.clone())
                .unwrap_or("N/A".to_string());

            UserModuleProgress {
                module: module_id,
                progress,
                duration,
            }
        })
        .collect()
}

#[derive(Debug, Serialize, Deserialize)]
enum ModuleProgress {
    NotStarted,
    Started,
    Completed,
}

#[derive(Debug, Serialize, Deserialize)]
struct UserModuleProgress {
    module: ObjectId,
    progress: ModuleProgress,
    duration: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct UserAnalytics {
    user: User,
    date_started: String,
    last_activity: String,
    date_completed: String,
    session_duration: String,
    groups_names: String,
    completed_modules_count: u32,
    active_modules_count: u32,
    completion_percentage: f32,
    modules: Vec<UserModuleProgress>,
}

fn get_user_analytics(
    user_opt: Option<&User>,
    user_groups_names: Option<&UserGroupsNames>,
    active_modules: &Vec<ActiveModule>,
    user_sessions_sprints: &UserSessionSprint,
    user_modules_durations: Option<&Vec<UserModuleDuration>>,
    user_completion_date: Option<&CompletionDate>,
) -> UserAnalytics {
    let (session_duration, user_modules_durations) = get_users_durations(user_modules_durations);

    let mut groups_names = vec![];
    if let Some(user_groups_names) = user_groups_names {
        groups_names = user_groups_names.groups.clone();
    }
    groups_names.sort_by(|a, b| a.cmp(b));
    let groups_names = groups_names.join("/");

    let mut date_completed = "N/A".to_string();
    if let Some(user_completion_date) = user_completion_date {
        date_completed = user_completion_date.date.to_string();
    }

    let modules = get_user_modules_analytics(
        &active_modules,
        &user_sessions_sprints.completed_modules_ids,
        &user_sessions_sprints.started_modules_ids,
        &user_modules_durations,
    );

    let mut user = User {
        _id: ObjectId::new(),
        email: "N/A".to_string(),
        lms_learner_id: "".to_string(),
    };
    if let Some(found) = user_opt {
        user = found.clone();
    }

    UserAnalytics {
        date_started: user_sessions_sprints.date_started.to_string(),
        last_activity: user_sessions_sprints.last_activity.to_string(),
        date_completed,
        session_duration,
        groups_names,
        user,
        completed_modules_count: user_sessions_sprints.completed_modules_count,
        active_modules_count: active_modules.len() as u32,
        completion_percentage: user_sessions_sprints.completion_percentage,
        modules,
    }
}

fn get_analytics(
    users_groups_names: &HashMap<ObjectId, UserGroupsNames>,
    active_modules: &Vec<ActiveModule>,
    users_session_sprints: &HashMap<ObjectId, UserSessionSprint>,
    completion_dates: &HashMap<ObjectId, CompletionDate>,
    users_modules_durations: &HashMap<ObjectId, Vec<UserModuleDuration>>,
    users: &HashMap<ObjectId, User>,
) -> Vec<UserAnalytics> {
    let start = Instant::now();

    let analytics = users_session_sprints
        .iter()
        .map(|(user_id, user_session_sprints)| {
            let user_groups_names = users_groups_names.get(user_id);
            let user_modules_durations = users_modules_durations.get(user_id);
            let user_completion_date = completion_dates.get(user_id);
            let user = users.get(user_id);

            get_user_analytics(
                user,
                user_groups_names,
                active_modules,
                user_session_sprints,
                user_modules_durations,
                user_completion_date,
            )
        })
        .collect();

    println!("  ➡️ get_analytics: {} ms", start.elapsed().as_millis());
    analytics
}

#[tokio::main]
async fn calc(db_name: &str, publication_id: &str) -> Result<Vec<UserAnalytics>, Box<dyn Error>> {
    // The array at the beginning of the line indicates what is required before the calculation (u
    // is for user and c for course)

    let start = Instant::now();

    let db = gt_db(db_name).await?;
    println!(
        "⏱️ {} ms - connection to MongoDB",
        start.elapsed().as_millis()
    );
    println!();

    let (users_groups_names, active_modules, users) = tokio::try_join!(
        get_users_groups_names(&db, publication_id), // TODO: add possiblity to filter for one user
        get_active_modules(&db, publication_id),
        get_users(&db),
    )?;
    println!(
        "⏱️ {} ms - users_groups_names {}, active_modules {} and users {}",
        start.elapsed().as_millis(),
        users_groups_names.len(),
        active_modules.len(),
        users.len()
    );
    println!();

    let active_modules_ids = active_modules
        .iter()
        .map(|m| m.module)
        .collect::<Vec<ObjectId>>();

    let (users_session_sprints, completion_dates, users_modules_durations) = tokio::try_join!(
        get_users_session_sprints(&db, &publication_id, &active_modules_ids),
        get_completion_dates(&db, &publication_id, &active_modules_ids),
        get_users_modules_durations(&db, &publication_id, &active_modules),
    )?;
    println!(
        "⏱️ {} ms - users_session_sprints {}, completion_dates {}, users modules durations {}",
        start.elapsed().as_millis(),
        users_session_sprints.len(),
        completion_dates.len(),
        users_modules_durations.len()
    );
    println!();

    let analytics = get_analytics(
        &users_groups_names,
        &active_modules,
        &users_session_sprints,
        &completion_dates,
        &users_modules_durations,
        &users,
    );

    println!(
        "⏱️ {} ms - analytics {}",
        start.elapsed().as_millis(),
        analytics.len()
    );

    Ok(analytics)
}

fn get_results(req: Request) -> Result<Vec<UserAnalytics>, Box<dyn Error>> {
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
            let data = serde_json::to_string(&list).unwrap();
            let mut compressed_data = GzEncoder::new(Vec::new(), Compression::default());
            compressed_data.write_all(data.as_bytes()).unwrap();

            let response = Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/json")
                .header("Content-Encoding", "gzip")
                .body(Body::from(compressed_data.finish().unwrap()))
                .expect("Internal Server Error");

            Ok(response)
        }
        Err(e) => Err(VercelError::new(&e.to_string())),
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
            .uri("https://api.example.com/analytics?db=org_afev&publication=60f19ade70889a488b569f1d")
            .body(Body::Empty)
            .unwrap();

        let result_option = get_results(req);
        assert!(result_option.is_ok());

        let result = result_option.unwrap();
        let data = serde_json::to_string(&result).unwrap();
        println!("data: {}", data.len());
        assert!(true)
    }
}
