extern crate uuid;
extern crate serde;
extern crate serde_json;
extern crate recap;
#[macro_use]
extern crate lazy_static;
use uuid::prelude::*;
use serde::{Deserialize};
use serde_json::{from_str};

use std::io;
use std::io::{stdin};
use std::io::BufRead;
use std::io::BufWriter;
use std::io::Write;

use recap::{Regex, from_captures};
use std::env;
use std::str::FromStr;
use std::result::Result as StdResult;

#[derive(Debug, Deserialize, PartialEq)]
struct EventLogEntryRaw {
    request_id: String,
    request_date: String,
    request_uri: String,
    x_real_ip: String,
    remote_addr: String,
    referrer: String,
    user_agent: String,
    accept_language: String,
    request_body: String,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct EventAggResult {
    item_id: Uuid,
    position: u32,
    item_type: String,
    track_id: Uuid,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct EventAggSearch {
    widget_id: Uuid,
    session_id: Uuid,
    user_id: Uuid,
    total: i32,
    event_id: Uuid,
    #[serde(rename = "type")]
    _type: String,
    query: String,
    filter: String,
    timestamp: u32,
    from: u32,
    size: u32,
    result: Vec<EventAggResult>,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
struct EventAggClick {
    #[serde(rename = "type")]
    _type: String,
    event_id: Uuid,
    track_id: Uuid,
    timestamp: u32,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
struct EventAggVisit {
    #[serde(rename = "type")]
    _type: String,
    event_id: Uuid,
    track_id: Uuid,
    timestamp: u32,
    time_elapsed: u32,
}

impl Tsv for EventAggResult {
    fn tsv(&self) -> String {
        [
            self.track_id.to_string(),
            self.item_id.to_string(),
            self.item_type.to_string(),
            self.position.to_string(),
        ].join("\t")
    }
}

impl Tsv for EventAggSearch {
    fn tsv(&self) -> String {
        return [
            self.timestamp.to_string(),
            self.event_id.to_string(),
            self.widget_id.to_string(),
            self.user_id.to_string(),
            self.session_id.to_string(),
            self.query.to_string(),
            self.filter.to_string(),
            self.from.to_string(),
            self.size.to_string(),
            self.total.to_string(),
        ].join("\t")
    }
}

impl EventAggSearch {
    fn tsv_results(&self) -> String {
        let mut vec:Vec<String> = Vec::new();
        for res in &self.result {
            vec.push([
                self.timestamp.to_string(),
                self.event_id.to_string(),
                res.tsv(),
            ].join("\t"));
        }
        return vec.join("\n");
    }
}

impl Tsv for EventAggVisit {
    fn tsv(&self) -> String {
        return [
            self.timestamp.to_string(),
            self.event_id.to_string(),
            self.track_id.to_string(),
            self.time_elapsed.to_string()
        ].join("\t");
    }
}

impl Tsv for EventAggClick {
    fn tsv(&self) -> String {
        return [
            self.timestamp.to_string(),
            self.event_id.to_string(),
            self.track_id.to_string()
        ].join("\t");
    }
}

pub trait Tsv {
    fn tsv(&self) -> String;
}



lazy_static! {
     static ref REG_EXP: Regex = Regex::new(r#"(?x)
 (?P<request_id>\S+)\s
 (?P<request_date>\S+)\s
 "(?P<request_uri>(?:\\"|[^"])*?)"\s
 "(?P<x_real_ip>(?:\\"|[^"])*?)"\s
 "(?P<remote_addr>(?:\\"|[^"])*?)"\s
 "(?P<referrer>(?:\\"|[^"])*?)"\s
 "(?P<user_agent>(?:\\"|[^"])*?)"\s
 "(?P<accept_language>(?:\\"|[^"])*?)"\s
 "(?P<request_body>(?:\\"|[^"])*?)"
 "#).unwrap();
    }



#[derive(Debug, PartialEq)]
enum EventTypes {
    Search,
    Result,
    Click,
    Visit,
}

impl FromStr for EventTypes {
    type Err = ();

    fn from_str(s: &str) -> StdResult<EventTypes, ()> {
        match s {
            "search" => Ok(EventTypes::Search),
            "result" => Ok(EventTypes::Result),
            "click" => Ok(EventTypes::Click),
            "visit" => Ok(EventTypes::Visit),
            _ => Err(()),
        }
    }
}


fn main() {
    let args: Vec<String> = env::args().collect();
    let event_type = &args[1].parse::<EventTypes>().unwrap();
//    let stdin = io::stdin();
//    let f = File::open("events.log").unwrap();
//    let file = BufReader::new(&f);
    let mut writer = BufWriter::new(io::stdout());
//    for line in file.lines() {
    for line in stdin().lock().lines() {
        let l = line.unwrap();
        let log_entry = from_captures::<EventLogEntryRaw>(&REG_EXP, &l);
        match log_entry {
            Ok(raw) => {
                let json_string = &raw.request_body.replace(r#"\""#, r#"""#);
                match event_type {
                    EventTypes::Result => {
                        let entry = from_str::<EventAggSearch>(
                            json_string
                        );
                        match entry {
                            Ok(e) =>  {
                                writer.write(e.tsv_results().as_bytes()).unwrap();
                                writer.write(b"\n").unwrap();
                            },
                            Err(_) => {
                            }
                        }
                    },
                    EventTypes::Search => {
                        let entry = from_str::<EventAggSearch>(
                            json_string
                        );
                        match entry {
                            Ok(e) =>  {
                                writer.write(e.tsv().as_bytes()).unwrap();
                                writer.write(b"\n").unwrap();
                            },
                            Err(_) => {
                            }
                        }
                    },
                    EventTypes::Click => {
                        let entry = from_str::<EventAggClick>(
                            json_string
                        );
                        match entry {
                            Ok(e) =>  {
                                writer.write(e.tsv().as_bytes()).unwrap();
                                writer.write(b"\n").unwrap();
                            },
                            Err(_) => {
                            }
                        }
                    },
                    EventTypes::Visit => {
                        let entry = from_str::<EventAggVisit>(
                            json_string
                        );
                        match entry {
                            Ok(e) =>  {
                                writer.write(e.tsv().as_bytes()).unwrap();
                                writer.write(b"\n").unwrap();
                            },
                            Err(_) => {
                            }
                        }
                    },
                }

            }
            Err(_) => {

            }
        }

    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EventTypes;

    #[test]
    fn test_event_type() {
        assert_eq!("search".parse::<EventTypes>().unwrap(), EventTypes::Search);
        assert_eq!("result".parse::<EventTypes>().unwrap(), EventTypes::Result);
        assert_eq!("click".parse::<EventTypes>().unwrap(), EventTypes::Click);
        assert_eq!("visit".parse::<EventTypes>().unwrap(), EventTypes::Visit);
    }

    #[test]
    fn de_visit() {
        assert_eq!(
            from_str::<EventAggVisit>(
                r#"{
                "type":"visit",
                "eventId":"941a496e-79a1-4f53-908e-d31cf1fc0c6e",
                "trackId":"292d83cb-f5f3-4fe3-8a5a-e7569f892369",
                "timestamp":1550758923,
                "timeElapsed":132
                }"#
            ).unwrap(),
            EventAggVisit {
                _type: "visit".to_string(),
                event_id: "941a496e-79a1-4f53-908e-d31cf1fc0c6e".parse::<Uuid>().unwrap(),
                track_id: "292d83cb-f5f3-4fe3-8a5a-e7569f892369".parse::<Uuid>().unwrap(),
                timestamp: 1550758923,
                time_elapsed: 132,
            }
        )
    }
}