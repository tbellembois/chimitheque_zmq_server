use chimitheque_db::{
    entity::get_entities,
    hazardstatement::get_hazard_statements,
    init::connect,
    person::get_people,
    precautionarystatement::get_precautionary_statements,
    producer::{create_update_producer, get_producers},
    producerref::get_producer_refs,
    product::get_products,
    pubchemproduct::create_update_product_from_pubchem,
    searchable::get_many,
    stock::compute_stock,
    storelocation::{create_update_store_location, delete_store_location, get_store_locations},
    supplier::{create_update_supplier, get_suppliers},
    supplierref::get_supplier_refs,
    unit::get_units,
    updatestatement::update_ghs_statements,
};
use chimitheque_types::{
    casnumber::CasNumber, category::Category, cenumber::CeNumber, classofcompound::ClassOfCompound,
    empiricalformula::EmpiricalFormula, linearformula::LinearFormula, name::Name,
    physicalstate::PhysicalState, producer::Producer, pubchemproduct::PubchemProduct,
    requestfilter::RequestFilter, signalword::SignalWord, storelocation::StoreLocation,
    supplier::Supplier, symbol::Symbol, tag::Tag,
};
use chimitheque_utils::{
    casnumber::is_cas_number,
    cenumber::is_ce_number,
    formula::sort_empirical_formula,
    pubchem::{autocomplete, get_compound_by_name, get_product_by_name},
    string::{clean, Transform},
};
use std::{num::NonZeroU32, path::Path};

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

    DBCreateUpdateProductFromPubchem(PubchemProduct, u64, Option<u64>),
    DBGetSuppliers(String),
    DBGetSupplierrefs(String),
    DBGetProducers(String),
    DBGetProducerrefs(String),
    DBGetCasnumbers(String),
    DBGetCenumbers(String),
    DBGetCategories(String),
    DBGetClassesofcompound(String),
    DBGetEmpiricalformulas(String),
    DBGetLinearformulas(String),
    DBGetHazardstatements(String),
    DBGetPrecautionarystatements(String),
    DBGetNames(String),
    DBGetPhysicalstates(String),
    DBGetSymbols(String),
    DBGetTags(String),
    DBGetSignalwords(String),
    DBGetUnits(String),

    DBComputeStock(u64, u64),

    DBGetStorelocations(String, u64),
    DBDeleteStorelocation(u64),
    DBGetEntities(String, u64),
    DBGetProducts(String, u64),
    DBGetPeople(String, u64),
    DBUpdateGHSStatements(String),

    DBCreateUpdateStorelocation(StoreLocation),
    DBCreateUpdateProducer(Producer),
    DBCreateUpdateSupplier(Supplier),
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
            debug!("maybe_raw_message: {:#?}", maybe_raw_message.as_str());

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
                                    Err(e) => Err(e.to_string()),
                                };
                            }
                            Request::RequestFilter(s) => {
                                info!("RequestFilter({s})");
                                response = match RequestFilter::try_from(s.as_str()) {
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
                                let mayerr_filter = RequestFilter::try_from(s.as_str());

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
                                let mayerr_filter = RequestFilter::try_from(s.as_str());

                                response = match mayerr_filter {
                                    Ok(filter) => match get_supplier_refs(&db_connection, filter) {
                                        Ok(o) => Ok(Box::new(o)),
                                        Err(e) => Err(e.to_string()),
                                    },
                                    Err(e) => Err(e),
                                };
                            }
                            Request::DBGetProducers(s) => {
                                info!("DBGetProducers({s})");
                                let mayerr_filter = RequestFilter::try_from(s.as_str());

                                response = match mayerr_filter {
                                    Ok(filter) => match get_producers(&db_connection, filter) {
                                        Ok(o) => Ok(Box::new(o)),
                                        Err(e) => Err(e.to_string()),
                                    },
                                    Err(e) => Err(e),
                                };
                            }
                            Request::DBGetProducerrefs(s) => {
                                info!("DBGetProducerrefs({s})");
                                let mayerr_filter = RequestFilter::try_from(s.as_str());

                                response = match mayerr_filter {
                                    Ok(filter) => match get_producer_refs(&db_connection, filter) {
                                        Ok(o) => Ok(Box::new(o)),
                                        Err(e) => Err(e.to_string()),
                                    },
                                    Err(e) => Err(e),
                                };
                            }
                            Request::DBGetCasnumbers(s) => {
                                info!("DBGetCasnumbers({s})");
                                let mayerr_filter = RequestFilter::try_from(s.as_str());

                                response = match mayerr_filter {
                                    Ok(filter) => match get_many(
                                        &CasNumber {
                                            ..Default::default()
                                        },
                                        &db_connection,
                                        filter,
                                    ) {
                                        Ok(o) => Ok(Box::new(o)),
                                        Err(e) => Err(e.to_string()),
                                    },
                                    Err(e) => Err(e),
                                };
                            }
                            Request::DBGetCenumbers(s) => {
                                info!("DBGetCenumbers({s})");
                                let mayerr_filter = RequestFilter::try_from(s.as_str());

                                response = match mayerr_filter {
                                    Ok(filter) => match get_many(
                                        &CeNumber {
                                            ..Default::default()
                                        },
                                        &db_connection,
                                        filter,
                                    ) {
                                        Ok(o) => Ok(Box::new(o)),
                                        Err(e) => Err(e.to_string()),
                                    },
                                    Err(e) => Err(e),
                                };
                            }
                            Request::DBGetCategories(s) => {
                                info!("DBGetCategories({s})");
                                let mayerr_filter = RequestFilter::try_from(s.as_str());

                                response = match mayerr_filter {
                                    Ok(filter) => match get_many(
                                        &Category {
                                            ..Default::default()
                                        },
                                        &db_connection,
                                        filter,
                                    ) {
                                        Ok(o) => Ok(Box::new(o)),
                                        Err(e) => Err(e.to_string()),
                                    },
                                    Err(e) => Err(e),
                                };
                            }
                            Request::DBGetClassesofcompound(s) => {
                                info!("DBGetClassesofcompound({s})");
                                let mayerr_filter = RequestFilter::try_from(s.as_str());

                                response = match mayerr_filter {
                                    Ok(filter) => match get_many(
                                        &ClassOfCompound {
                                            ..Default::default()
                                        },
                                        &db_connection,
                                        filter,
                                    ) {
                                        Ok(o) => Ok(Box::new(o)),
                                        Err(e) => Err(e.to_string()),
                                    },
                                    Err(e) => Err(e),
                                };
                            }
                            Request::DBGetEmpiricalformulas(s) => {
                                info!("DBGetEmpiricalformulas({s})");
                                let mayerr_filter = RequestFilter::try_from(s.as_str());

                                response = match mayerr_filter {
                                    Ok(filter) => match get_many(
                                        &EmpiricalFormula {
                                            ..Default::default()
                                        },
                                        &db_connection,
                                        filter,
                                    ) {
                                        Ok(o) => Ok(Box::new(o)),
                                        Err(e) => Err(e.to_string()),
                                    },
                                    Err(e) => Err(e),
                                };
                            }
                            Request::DBGetLinearformulas(s) => {
                                info!("DBGetLinearformulas({s})");
                                let mayerr_filter = RequestFilter::try_from(s.as_str());

                                response = match mayerr_filter {
                                    Ok(filter) => match get_many(
                                        &LinearFormula {
                                            ..Default::default()
                                        },
                                        &db_connection,
                                        filter,
                                    ) {
                                        Ok(o) => Ok(Box::new(o)),
                                        Err(e) => Err(e.to_string()),
                                    },
                                    Err(e) => Err(e),
                                };
                            }
                            Request::DBGetHazardstatements(s) => {
                                info!("DBGetHazardstatements({s})");
                                let mayerr_filter = RequestFilter::try_from(s.as_str());

                                response = match mayerr_filter {
                                    Ok(filter) => {
                                        match get_hazard_statements(&db_connection, filter) {
                                            Ok(o) => Ok(Box::new(o)),
                                            Err(e) => Err(e.to_string()),
                                        }
                                    }
                                    Err(e) => Err(e),
                                };
                            }
                            Request::DBGetPrecautionarystatements(s) => {
                                info!("DBGetPrecautionarystatements({s})");
                                let mayerr_filter = RequestFilter::try_from(s.as_str());

                                response = match mayerr_filter {
                                    Ok(filter) => {
                                        match get_precautionary_statements(&db_connection, filter) {
                                            Ok(o) => Ok(Box::new(o)),
                                            Err(e) => Err(e.to_string()),
                                        }
                                    }
                                    Err(e) => Err(e),
                                };
                            }
                            Request::DBGetNames(s) => {
                                info!("DBGetNames({s})");
                                let mayerr_filter = RequestFilter::try_from(s.as_str());

                                response = match mayerr_filter {
                                    Ok(filter) => match get_many(
                                        &Name {
                                            ..Default::default()
                                        },
                                        &db_connection,
                                        filter,
                                    ) {
                                        Ok(o) => Ok(Box::new(o)),
                                        Err(e) => Err(e.to_string()),
                                    },
                                    Err(e) => Err(e),
                                };
                            }
                            Request::DBGetPhysicalstates(s) => {
                                info!("DBGetPhysicalstates({s})");
                                let mayerr_filter = RequestFilter::try_from(s.as_str());

                                response = match mayerr_filter {
                                    Ok(filter) => match get_many(
                                        &PhysicalState {
                                            ..Default::default()
                                        },
                                        &db_connection,
                                        filter,
                                    ) {
                                        Ok(o) => Ok(Box::new(o)),
                                        Err(e) => Err(e.to_string()),
                                    },
                                    Err(e) => Err(e),
                                };
                            }
                            Request::DBGetSymbols(s) => {
                                info!("DBGetSymbols({s})");
                                let mayerr_filter = RequestFilter::try_from(s.as_str());

                                response = match mayerr_filter {
                                    Ok(filter) => match get_many(
                                        &Symbol {
                                            ..Default::default()
                                        },
                                        &db_connection,
                                        filter,
                                    ) {
                                        Ok(o) => Ok(Box::new(o)),
                                        Err(e) => Err(e.to_string()),
                                    },
                                    Err(e) => Err(e),
                                };
                            }
                            Request::DBGetTags(s) => {
                                info!("DBGetTags({s})");
                                let mayerr_filter = RequestFilter::try_from(s.as_str());

                                response = match mayerr_filter {
                                    Ok(filter) => match get_many(
                                        &Tag {
                                            ..Default::default()
                                        },
                                        &db_connection,
                                        filter,
                                    ) {
                                        Ok(o) => Ok(Box::new(o)),
                                        Err(e) => Err(e.to_string()),
                                    },
                                    Err(e) => Err(e),
                                };
                            }
                            Request::DBGetUnits(s) => {
                                info!("DBGetUnits({s})");
                                let mayerr_filter = RequestFilter::try_from(s.as_str());

                                response = match mayerr_filter {
                                    Ok(filter) => match get_units(&db_connection, filter) {
                                        Ok(o) => Ok(Box::new(o)),
                                        Err(e) => Err(e.to_string()),
                                    },
                                    Err(e) => Err(e),
                                };
                            }
                            Request::DBGetSignalwords(s) => {
                                info!("DBGetSignalwords({s})");
                                let mayerr_filter = RequestFilter::try_from(s.as_str());

                                response = match mayerr_filter {
                                    Ok(filter) => match get_many(
                                        &SignalWord {
                                            ..Default::default()
                                        },
                                        &db_connection,
                                        filter,
                                    ) {
                                        Ok(o) => Ok(Box::new(o)),
                                        Err(e) => Err(e.to_string()),
                                    },
                                    Err(e) => Err(e),
                                };
                            }
                            Request::DBGetEntities(s, person_id) => {
                                info!("DBGetEntities({s} {person_id})");
                                let mayerr_filter = RequestFilter::try_from(s.as_str());

                                response = match mayerr_filter {
                                    Ok(filter) => {
                                        match get_entities(&db_connection, filter, person_id) {
                                            Ok(o) => Ok(Box::new(o)),
                                            Err(e) => Err(e.to_string()),
                                        }
                                    }
                                    Err(e) => Err(e),
                                };
                            }
                            Request::DBGetStorelocations(s, person_id) => {
                                info!("DBGetStoreLocations({s} {person_id})");
                                let mayerr_filter = RequestFilter::try_from(s.as_str());

                                response = match mayerr_filter {
                                    Ok(filter) => {
                                        match get_store_locations(&db_connection, filter, person_id)
                                        {
                                            Ok(o) => Ok(Box::new(o)),
                                            Err(e) => Err(e.to_string()),
                                        }
                                    }
                                    Err(e) => Err(e),
                                };
                            }
                            Request::DBDeleteStorelocation(store_location_id) => {
                                info!("DBDeleteStoreLocation({store_location_id})");

                                response = match delete_store_location(
                                    &db_connection,
                                    store_location_id,
                                ) {
                                    Ok(o) => Ok(Box::new(o)),
                                    Err(e) => Err(e.to_string()),
                                };
                            }
                            Request::DBGetProducts(s, person_id) => {
                                info!("DBGetProducts({s} {person_id})");
                                let mayerr_filter = RequestFilter::try_from(s.as_str());

                                response = match mayerr_filter {
                                    Ok(filter) => {
                                        match get_products(&db_connection, filter, person_id) {
                                            Ok(o) => Ok(Box::new(o)),
                                            Err(e) => Err(e.to_string()),
                                        }
                                    }
                                    Err(e) => Err(e),
                                };
                            }
                            Request::DBGetPeople(s, person_id) => {
                                info!("DBGetPeople({s} {person_id})");
                                let mayerr_filter = RequestFilter::try_from(s.as_str());

                                response = match mayerr_filter {
                                    Ok(filter) => {
                                        match get_people(&db_connection, filter, person_id) {
                                            Ok(o) => Ok(Box::new(o)),
                                            Err(e) => Err(e.to_string()),
                                        }
                                    }
                                    Err(e) => Err(e),
                                };
                            }
                            Request::DBCreateUpdateProductFromPubchem(
                                pubchem_product,
                                person_id,
                                maybe_product_id,
                            ) => {
                                info!(
                                    "DBCreateUpdateProductFromPubchem({:?} {} {:?})",
                                    pubchem_product, person_id, maybe_product_id
                                );

                                response = match create_update_product_from_pubchem(
                                    &db_connection,
                                    pubchem_product,
                                    person_id,
                                    maybe_product_id,
                                ) {
                                    Ok(product_id) => Ok(Box::new(product_id)),
                                    Err(e) => Err(e.to_string()),
                                }
                            }
                            Request::DBUpdateGHSStatements(_s) => {
                                info!("DBUpdateGHSStatements");

                                response = match update_ghs_statements(&db_connection) {
                                    Ok(_) => Ok(Box::new(())),
                                    Err(e) => Err(e.to_string()),
                                }
                            }
                            Request::DBCreateUpdateStorelocation(store_location) => {
                                info!("DBCreateUpdateStorelocation({:?})", store_location);

                                let mut clean_store_location = store_location.clone();
                                clean_store_location.store_location_name =
                                    clean(&store_location.store_location_name, Transform::None);

                                response = match create_update_store_location(
                                    &db_connection,
                                    clean_store_location,
                                ) {
                                    Ok(store_location_id) => Ok(Box::new(store_location_id)),
                                    Err(e) => Err(e.to_string()),
                                }
                            }
                            Request::DBComputeStock(product_id, person_id) => {
                                info!("DBComputeStock({} {})", product_id, person_id);

                                response =
                                    match compute_stock(&db_connection, product_id, person_id) {
                                        Ok(o) => Ok(Box::new(o)),
                                        Err(e) => Err(e.to_string()),
                                    }
                            }
                            Request::DBCreateUpdateProducer(producer) => {
                                info!("DBCreateUpdateProducer({:?})", producer);

                                let mut clean_producer = producer.clone();
                                clean_producer.producer_label =
                                    clean(&producer.producer_label, Transform::None);

                                response =
                                    match create_update_producer(&db_connection, clean_producer) {
                                        Ok(producer_id) => Ok(Box::new(producer_id)),
                                        Err(e) => Err(e.to_string()),
                                    }
                            }
                            Request::DBCreateUpdateSupplier(supplier) => {
                                info!("DBCreateUpdateSupplier({:?})", supplier);

                                let mut clean_supplier = supplier.clone();
                                clean_supplier.supplier_label =
                                    clean(&supplier.supplier_label, Transform::None);

                                response =
                                    match create_update_supplier(&db_connection, clean_supplier) {
                                        Ok(supplier_id) => Ok(Box::new(supplier_id)),
                                        Err(e) => Err(e.to_string()),
                                    }
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
                    debug!("response: {}", serialized_response);
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
