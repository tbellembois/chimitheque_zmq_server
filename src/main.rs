use std::{num::NonZeroU32, path::Path};

use chimitheque_db::{init::connect, supplier::get_suppliers, supplierref::get_supplierrefs};
use chimitheque_utils::{
    casnumber::is_cas_number,
    cenumber::is_ce_number,
    formula::sort_empirical_formula,
    pubchem::{autocomplete, get_compound_by_name, get_product_by_name},
    requestfilter::request_filter,
};

use clap::Parser;
use governor::{Quota, RateLimiter};
use log::{debug, error, info};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
enum Request {
    IsCasNumber(String),
    IsCeNumber(String),
    SortEmpiricalFormula(String),
    RequestFilter(String),
    PubchemAutocomplete(String),
    PubchemGetCompoundByName(String),
    PubchemGetProductByName(String),

    DBGetSuppliers(String),
    DBGetSupplierrefs(String),
}

#[derive(Parser)]
struct Cli {
    /// Chimithèque database full path. example: /path/to/storage.db
    #[arg(long)]
    db_path: String,
}

fn main() {
    env_logger::init();

    // Read parameters.
    let cli = Cli::parse();

    debug!("arg {}", cli.db_path);

    // Check that the path exist.
    if !Path::new(&cli.db_path).is_file() {
        error!("path {} is not a file", &cli.db_path);
        return;
    }

    // Create a connection.
    let db_connection = connect(&cli.db_path).unwrap();

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
                            Request::SortEmpiricalFormula(s) => {
                                info!("SortEmpiricalFormula({s})");
                                response = match sort_empirical_formula(&s) {
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
                            Request::PubchemAutocomplete(s) => {
                                info!("PubchemAutocomplete({s})");
                                response = match autocomplete(&rate_limiter, &s) {
                                    Ok(o) => Ok(Box::new(o)),
                                    Err(e) => Err(e),
                                };
                            }
                            Request::PubchemGetCompoundByName(s) => {
                                info!("PubchemGetCompoundByName({s})");
                                response = match get_compound_by_name(&rate_limiter, &s) {
                                    Ok(o) => Ok(Box::new(o)),
                                    Err(e) => Err(e),
                                };
                            }
                            Request::PubchemGetProductByName(s) => {
                                info!("PubchemGetProductByName({s})");
                                response = match get_product_by_name(&rate_limiter, &s) {
                                    Ok(o) => Ok(Box::new(o)),
                                    Err(e) => Err(e),
                                };
                            }
                            Request::DBGetSuppliers(s) => {
                                info!("DBGetSuppliers({s})");
                                let mayerr_filter = request_filter(&s);

                                response = match mayerr_filter {
                                    Ok(filter) => match get_suppliers(&db_connection, filter) {
                                        Ok(o) => Ok(Box::new(o)),
                                        Err(e) => Err(e.to_string()),
                                    },
                                    Err(e) => Err(e),
                                };
                            }
                            Request::DBGetSupplierrefs(s) => {
                                info!("DBGetSupplierrefs({s})");
                                let mayerr_filter = request_filter(&s);

                                response = match mayerr_filter {
                                    Ok(filter) => match get_supplierrefs(&db_connection, filter) {
                                        Ok(o) => Ok(Box::new(o)),
                                        Err(e) => Err(e.to_string()),
                                    },
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
