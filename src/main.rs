use std::num::NonZeroU32;

use chimitheque_utils::{
    casnumber::is_cas_number,
    cenumber::is_ce_number,
    formula::empirical_formula,
    pubchem::{autocomplete, get_compound_by_name},
    requestfilter::request_filter,
};

use governor::{Quota, RateLimiter};
use log::{debug, error, info};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
enum Request {
    IsCasNumber(String),
    IsCeNumber(String),
    EmpiricalFormula(String),
    RequestFilter(String),
    Autocomplete(String),
    GetCompoundByName(String),
}

fn main() {
    env_logger::init();

    // Initialize rate limiter for pubchem requests.
    let rate_limiter = RateLimiter::direct(Quota::per_second(NonZeroU32::new(5).unwrap()));

    // Connect to socket.
    let context = zmq::Context::new();
    let responder = context.socket(zmq::REP).unwrap();
    assert!(responder.bind("tcp://*:5556").is_ok());

    // Input message variable.
    let mut maybe_raw_message = zmq::Message::new();

    info!("Waiting for messages.");

    loop {
        // Waiting for incoming messages.
        if let Err(e) = responder.recv(&mut maybe_raw_message, 0) {
            error!("error receiving message: {e}");
        } else {
            debug!("maybe_raw_message: {:?}", maybe_raw_message.as_str());

            // FIXME: change default response.
            let mut response: Result<Box<dyn erased_serde::Serialize>, String> =
                Err("empty response".to_string());

            // Decoding message into Request.
            if let Some(message) = maybe_raw_message.as_str() {
                match serde_json::from_str::<Request>(message) {
                    Ok(request) => {
                        info!("request: {:?}", request);

                        // Do the job.
                        match request {
                            Request::IsCasNumber(s) => {
                                info!("IsCasNumber({s})");
                                response = match is_cas_number(&s) {
                                    Ok(o) => Ok(Box::new(o)),
                                    Err(e) => Err(e),
                                };
                            }
                            Request::IsCeNumber(s) => {
                                info!("IsCeNumber({s})");
                                response = match is_ce_number(&s) {
                                    Ok(o) => Ok(Box::new(o)),
                                    Err(e) => Err(e),
                                };
                            }
                            Request::EmpiricalFormula(s) => {
                                info!("EmpiricalFormula({s})");
                                response = match empirical_formula(&s) {
                                    Ok(o) => Ok(Box::new(o)),
                                    Err(e) => Err(e),
                                };
                            }
                            Request::RequestFilter(s) => {
                                info!("RequestFilter({s})");
                                response = match request_filter(&s) {
                                    Ok(o) => Ok(Box::new(o)),
                                    Err(e) => Err(e),
                                };
                            }
                            Request::Autocomplete(s) => {
                                info!("Autocomplete({s})");
                                response = match autocomplete(&rate_limiter, &s) {
                                    Ok(o) => Ok(Box::new(o)),
                                    Err(e) => Err(e),
                                };
                            }
                            Request::GetCompoundByName(s) => {
                                info!("GetcompoundByName({s})");
                                response = match get_compound_by_name(&rate_limiter, &s) {
                                    Ok(o) => Ok(Box::new(o)),
                                    Err(e) => Err(e),
                                };
                            }
                        }
                    }
                    Err(e) => {
                        error!("error deserializing message: {e}");
                        response = Err(e.to_string());
                    }
                };
            }

            // Serialize response.
            match serde_json::to_string(&response) {
                Ok(serialized_response) => {
                    info!("response: {:#?}", serialized_response);
                    if let Err(e) = responder.send(&serialized_response, 0) {
                        error!("error sending response: {e}");
                    };
                }
                Err(e) => {
                    error!("error serializing response: {e}");
                    if let Err(e) = responder.send(&e.to_string(), 0) {
                        error!("error sending response: {e}");
                    };
                }
            };
        }
    }
}
