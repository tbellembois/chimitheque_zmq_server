use chimitheque_db::{
    bookmark::toggle_product_bookmark,
    borrowing::toggle_storage_borrowing,
    casbin::{
        match_entity_has_members, match_entity_has_store_locations, match_person_is_admin,
        match_person_is_in_entity, match_person_is_in_person_entity,
        match_person_is_in_storage_entity, match_person_is_in_store_location_entity,
        match_person_is_manager, match_product_has_storages, match_storage_is_in_entity,
        match_store_location_has_children, match_store_location_has_storages,
        match_store_location_is_in_entity,
    },
    entity::{create_update_entity, delete_entity, get_entities},
    hazardstatement::get_hazard_statements,
    init::{connect, init_db, update_ghs_statements},
    person::{
        create_update_person, delete_person, get_admins, get_people, set_person_admin,
        unset_person_admin,
    },
    precautionarystatement::get_precautionary_statements,
    producer::get_producers,
    producerref::get_producer_refs,
    product::{create_update_product, delete_product, export_products, get_products},
    pubchemproduct::create_update_product_from_pubchem,
    searchable::{self, get_many},
    stock::compute_stock,
    storage::{
        archive_storage, create_update_storage, delete_storage, export_storages, get_storages,
        unarchive_storage,
    },
    storelocation::{create_update_store_location, delete_store_location, get_store_locations},
    supplier::get_suppliers,
    supplierref::get_supplier_refs,
    unit::get_units,
};
use chimitheque_pubchem::pubchem::{autocomplete, get_compound_by_name, get_product_by_name};
use chimitheque_types::{
    casnumber::CasNumber, category::Category, cenumber::CeNumber, classofcompound::ClassOfCompound,
    empiricalformula::EmpiricalFormula, entity::Entity, linearformula::LinearFormula, name::Name,
    person::Person, physicalstate::PhysicalState, producer::Producer, product::Product,
    pubchemproduct::PubchemProduct, requestfilter::RequestFilter, signalword::SignalWord,
    storage::Storage, storelocation::StoreLocation, supplier::Supplier, symbol::Symbol, tag::Tag,
};
use chimitheque_utils::{
    casnumber::is_cas_number,
    cenumber::is_ce_number,
    formula::sort_empirical_formula,
    string::{clean, Transform},
};
use std::{
    fmt::{Display, Formatter},
    num::NonZeroU32,
    os::unix::fs::MetadataExt,
    path::Path,
};

use clap::Parser;
use governor::{Quota, RateLimiter};
use log::{debug, error, info};
use serde::Deserialize;

#[derive(Debug, PartialEq, Eq)]
pub enum ValidationError {
    InvalidEmailAddress(String, String),
}

impl Display for ValidationError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            ValidationError::InvalidEmailAddress(email, reason) => {
                write!(f, "invalid email address: {} ({})", email, reason)
            }
        }
    }
}

impl std::error::Error for ValidationError {}

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
    DBGetStorages(String, u64),
    DBGetPeople(String, u64),
    DBCreateUpdateEntity(Entity),

    DBCreateUpdateStorelocation(StoreLocation),
    DBCreateUpdateProduct(Product),
    DBCreateUpdateStorage(Storage, u64, bool),
    DBCreateUpdateProducer(Producer),
    DBCreateUpdateSupplier(Supplier),
    DBCreateUpdatePerson(Person),
    DBSetPersonAdmin(u64),
    DBUnsetPersonAdmin(u64),
    DBDeletePerson(u64),
    DBDeleteProduct(u64),
    DBDeleteStorage(u64),
    DBDeleteEntity(u64),
    DBArchiveStorage(u64),
    DBUnarchiveStorage(u64),
    DBGetAdmins(String),

    DBToggleProductBookmark(u64, u64),
    DBToggleStorageBorrowing(u64, u64, u64, Option<String>),

    CasbinMatchPersonIsInEntity(u64, u64),
    CasbinMatchPersonIsInPersonEntity(u64, u64),
    CasbinMatchPersonIsInStoreLocationEntity(u64, u64),
    CasbinMatchPersonIsInStorageEntity(u64, u64),
    CasbinMatchProductHasStorages(u64),
    CasbinMatchStoreLocationHasChildren(u64),
    CasbinMatchStoreLocationHasStorages(u64),
    CasbinMatchPersonIsAdmin(u64),
    CasbinMatchPersonIsManager(u64),
    CasbinMatchEntityHasMembers(u64),
    CasbinMatchEntityHasStoreLocations(u64),
    CasbinMatchStorageIsInEntity(u64, u64),
    CasbinMatchStoreLocationIsInEntity(u64, u64),

    DBExportProducts(String, u64),
    DBExportStorages(String, u64),
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

    // Create a connection.
    info!("Connecting to DB.");
    let mut db_connection = connect(&cli.db_path).unwrap();

    // Check that DB file exist, create if not..
    if Path::new(&cli.db_path).metadata().unwrap().size() == 0 {
        init_db(&mut db_connection).expect("can not init DB");
    } else {
        // Updating statements on already existing DB - panic on failure.
        let db_transaction = db_connection.transaction().unwrap();

        update_ghs_statements(&db_transaction).unwrap();

        db_transaction.commit().unwrap();
    }

    // Handle Ctrl+C.
    // ctrlc::set_handler(move || {
    //     // Close DB connection.
    //     match db_connection.close() {
    //         Ok(_) => info!("Closing DB."),
    //         Err(e) => error!("Error closing DB: {:?}", e),
    //     }
    // })
    // .expect("Error setting Ctrl-C handler");

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
                                    Err(e) => Err(e.to_string()),
                                };
                            }
                            Request::IsCeNumber(s) => {
                                info!("IsCeNumber({s})");
                                response = match is_ce_number(&s) {
                                    Ok(o) => Ok(Box::new(o)),
                                    Err(e) => Err(e.to_string()),
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
                            Request::DBExportProducts(s, person_id) => {
                                info!("DBExportProducts({s} {person_id})");
                                let mayerr_filter = RequestFilter::try_from(s.as_str());

                                response = match mayerr_filter {
                                    Ok(filter) => {
                                        match export_products(&db_connection, filter, person_id) {
                                            Ok(o) => Ok(Box::new(o)),
                                            Err(e) => Err(e.to_string()),
                                        }
                                    }
                                    Err(e) => Err(e),
                                };
                            }
                            Request::DBGetStorages(s, person_id) => {
                                info!("DBGetStorages({s} {person_id})");
                                let mayerr_filter = RequestFilter::try_from(s.as_str());

                                response = match mayerr_filter {
                                    Ok(filter) => {
                                        match get_storages(&db_connection, filter, person_id) {
                                            Ok(o) => Ok(Box::new(o)),
                                            Err(e) => Err(e.to_string()),
                                        }
                                    }
                                    Err(e) => Err(e),
                                };
                            }
                            Request::DBExportStorages(s, person_id) => {
                                info!("DBExportStorages({s} {person_id})");
                                let mayerr_filter = RequestFilter::try_from(s.as_str());

                                response = match mayerr_filter {
                                    Ok(filter) => {
                                        match export_storages(&db_connection, filter, person_id) {
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
                                    &mut db_connection,
                                    pubchem_product,
                                    person_id,
                                    maybe_product_id,
                                ) {
                                    Ok(product_id) => Ok(Box::new(product_id)),
                                    Err(e) => Err(e.to_string()),
                                };
                            }
                            Request::DBCreateUpdateProduct(product) => {
                                info!("DBCreateUpdateProduct({:?})", product);

                                let mut mayerr_sanitized_and_validated_product =
                                    product.clone().sanitize_and_validate();

                                match mayerr_sanitized_and_validated_product {
                                    Ok(sanitized_and_validated_product) => {
                                        response = match create_update_product(
                                            &mut db_connection,
                                            sanitized_and_validated_product,
                                        ) {
                                            Ok(product_id) => Ok(Box::new(product_id)),
                                            Err(e) => Err(e.to_string()),
                                        }
                                    }
                                    Err(err) => response = Err(err.to_string()),
                                }
                            }
                            Request::DBCreateUpdatePerson(person) => {
                                info!("DBCreateUpdatePerson({:?})", person);

                                let mut mayerr_sanitized_and_validated_person =
                                    person.clone().sanitize_and_validate();

                                match mayerr_sanitized_and_validated_person {
                                    Ok(sanitized_and_validated_person) => {
                                        response = match create_update_person(
                                            &mut db_connection,
                                            sanitized_and_validated_person,
                                        ) {
                                            Ok(person_id) => Ok(Box::new(person_id)),
                                            Err(e) => Err(e.to_string()),
                                        }
                                    }
                                    Err(err) => response = Err(err.to_string()),
                                }
                            }
                            Request::DBCreateUpdateStorage(
                                storage,
                                nb_items,
                                identical_barecode,
                            ) => {
                                info!("DBCreateUpdateStorage({:?})", storage);

                                response = match create_update_storage(
                                    &mut db_connection,
                                    storage,
                                    nb_items,
                                    identical_barecode,
                                ) {
                                    Ok(storage_ids) => Ok(Box::new(storage_ids)),
                                    Err(e) => Err(e.to_string()),
                                }
                            }
                            Request::DBCreateUpdateStorelocation(store_location) => {
                                info!("DBCreateUpdateStorelocation({:?})", store_location);

                                let mut sanitized_and_validated_store_location =
                                    store_location.clone().sanitize_and_validate();

                                response = match create_update_store_location(
                                    &mut db_connection,
                                    sanitized_and_validated_store_location,
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

                                let mut sanitized_and_validated_producer =
                                    producer.clone().sanitize_and_validate();

                                response = match searchable::create_update(
                                    &Producer {
                                        ..Default::default()
                                    },
                                    None,
                                    &db_connection,
                                    &sanitized_and_validated_producer.producer_label,
                                    Transform::None,
                                ) {
                                    Ok(producer_id) => Ok(Box::new(producer_id)),
                                    Err(e) => Err(e.to_string()),
                                }
                            }
                            Request::DBCreateUpdateSupplier(supplier) => {
                                info!("DBCreateUpdateSupplier({:?})", supplier);

                                let mut sanitized_and_validated_supplier =
                                    supplier.clone().sanitize_and_validate();

                                response = match searchable::create_update(
                                    &Supplier {
                                        ..Default::default()
                                    },
                                    None,
                                    &db_connection,
                                    &sanitized_and_validated_supplier.supplier_label,
                                    Transform::None,
                                ) {
                                    Ok(supplier_id) => Ok(Box::new(supplier_id)),
                                    Err(e) => Err(e.to_string()),
                                }
                            }
                            Request::DBToggleProductBookmark(person_id, product_id) => {
                                info!("DBToggleProductBookmark({:?},{:?})", person_id, product_id);

                                response = match toggle_product_bookmark(
                                    &mut db_connection,
                                    person_id,
                                    product_id,
                                ) {
                                    Ok(()) => Ok(Box::new(())),
                                    Err(e) => Err(e.to_string()),
                                }
                            }
                            Request::DBToggleStorageBorrowing(
                                person_id,
                                storage_id,
                                borrower_id,
                                borrower_comment,
                            ) => {
                                info!(
                                    "DBToggleStorageBorrowing({:?},{:?},{:?},{:?})",
                                    person_id, storage_id, borrower_id, borrower_comment
                                );

                                response = match toggle_storage_borrowing(
                                    &mut db_connection,
                                    person_id,
                                    storage_id,
                                    borrower_id,
                                    borrower_comment,
                                ) {
                                    Ok(()) => Ok(Box::new(())),
                                    Err(e) => Err(e.to_string()),
                                }
                            }
                            Request::DBSetPersonAdmin(person_id) => {
                                info!("DBSetPersonAdmin({:?})", person_id);

                                response = match set_person_admin(&mut db_connection, person_id) {
                                    Ok(()) => Ok(Box::new(())),
                                    Err(e) => Err(e.to_string()),
                                };
                            }
                            Request::DBUnsetPersonAdmin(person_id) => {
                                info!("DBUnsetPersonAdmin({:?})", person_id);

                                response = match unset_person_admin(&mut db_connection, person_id) {
                                    Ok(()) => Ok(Box::new(())),
                                    Err(e) => Err(e.to_string()),
                                };
                            }
                            Request::DBCreateUpdateEntity(entity) => {
                                info!("DBCreateUpdateEntity({:?})", entity);

                                let mut sanitized_and_validated_entity =
                                    entity.clone().sanitize_and_validate();

                                response = match create_update_entity(
                                    &mut db_connection,
                                    sanitized_and_validated_entity,
                                ) {
                                    Ok(entity_id) => Ok(Box::new(entity_id)),
                                    Err(e) => Err(e.to_string()),
                                };
                            }
                            Request::DBDeletePerson(person_id) => {
                                info!("DBDeletePerson({:?})", person_id);

                                response = match delete_person(&mut db_connection, person_id) {
                                    Ok(()) => Ok(Box::new(())),
                                    Err(e) => Err(e.to_string()),
                                };
                            }
                            Request::DBDeleteProduct(product_id) => {
                                info!("DBDeleteProduct({:?})", product_id);

                                response = match delete_product(&mut db_connection, product_id) {
                                    Ok(()) => Ok(Box::new(())),
                                    Err(e) => Err(e.to_string()),
                                };
                            }
                            Request::DBDeleteStorage(storage_id) => {
                                info!("DBDeleteStorage({:?})", storage_id);

                                response = match delete_storage(&mut db_connection, storage_id) {
                                    Ok(()) => Ok(Box::new(())),
                                    Err(e) => Err(e.to_string()),
                                };
                            }
                            Request::DBArchiveStorage(storage_id) => {
                                info!("DBArchiveStorage({:?})", storage_id);

                                response = match archive_storage(&mut db_connection, storage_id) {
                                    Ok(()) => Ok(Box::new(())),
                                    Err(e) => Err(e.to_string()),
                                };
                            }
                            Request::DBUnarchiveStorage(storage_id) => {
                                info!("DBUnarchiveStorage({:?})", storage_id);

                                response = match unarchive_storage(&mut db_connection, storage_id) {
                                    Ok(()) => Ok(Box::new(())),
                                    Err(e) => Err(e.to_string()),
                                };
                            }
                            Request::DBGetAdmins(_s) => {
                                info!("DBGetAdmins()");

                                response = match get_admins(&mut db_connection) {
                                    Ok(o) => Ok(Box::new(o)),
                                    Err(e) => Err(e.to_string()),
                                };
                            }
                            Request::CasbinMatchPersonIsInEntity(person_id, entity_id) => {
                                info!(
                                    "CasbinMatchPersonIsInEntity({:?},{:?})",
                                    person_id, entity_id
                                );

                                response = match match_person_is_in_entity(
                                    &db_connection,
                                    person_id,
                                    entity_id,
                                ) {
                                    Ok(o) => Ok(Box::new(o)),
                                    Err(e) => Err(e.to_string()),
                                };
                            }
                            Request::CasbinMatchPersonIsInPersonEntity(
                                person_id,
                                other_person_id,
                            ) => {
                                info!(
                                    "CasbinMatchPersonIsInPersonEntity({:?},{:?})",
                                    person_id, other_person_id
                                );

                                response = match match_person_is_in_person_entity(
                                    &db_connection,
                                    person_id,
                                    other_person_id,
                                ) {
                                    Ok(o) => Ok(Box::new(o)),
                                    Err(e) => Err(e.to_string()),
                                };
                            }
                            Request::CasbinMatchPersonIsInStoreLocationEntity(
                                person_id,
                                store_location_id,
                            ) => {
                                info!(
                                    "CasbinMatchPersonIsInStoreLocationEntity({:?},{:?})",
                                    person_id, store_location_id
                                );

                                response = match match_person_is_in_store_location_entity(
                                    &db_connection,
                                    person_id,
                                    store_location_id,
                                ) {
                                    Ok(o) => Ok(Box::new(o)),
                                    Err(e) => Err(e.to_string()),
                                };
                            }
                            Request::CasbinMatchPersonIsInStorageEntity(person_id, storage_id) => {
                                info!(
                                    "CasbinMatchPersonIsInStorageEntity({:?},{:?})",
                                    person_id, storage_id
                                );

                                response = match match_person_is_in_storage_entity(
                                    &db_connection,
                                    person_id,
                                    storage_id,
                                ) {
                                    Ok(o) => Ok(Box::new(o)),
                                    Err(e) => Err(e.to_string()),
                                };
                            }
                            Request::CasbinMatchProductHasStorages(product_id) => {
                                info!("CasbinMatchProductHasStorages({:?})", product_id);

                                response =
                                    match match_product_has_storages(&db_connection, product_id) {
                                        Ok(o) => Ok(Box::new(o)),
                                        Err(e) => Err(e.to_string()),
                                    };
                            }
                            Request::CasbinMatchStoreLocationHasChildren(store_location_id) => {
                                info!(
                                    "CasbinMatchStoreLocationHasChildren({:?})",
                                    store_location_id
                                );

                                response = match match_store_location_has_children(
                                    &db_connection,
                                    store_location_id,
                                ) {
                                    Ok(o) => Ok(Box::new(o)),
                                    Err(e) => Err(e.to_string()),
                                };
                            }
                            Request::CasbinMatchStoreLocationHasStorages(store_location_id) => {
                                info!(
                                    "CasbinMatchStoreLocationHasStorages({:?})",
                                    store_location_id
                                );

                                response = match match_store_location_has_storages(
                                    &db_connection,
                                    store_location_id,
                                ) {
                                    Ok(o) => Ok(Box::new(o)),
                                    Err(e) => Err(e.to_string()),
                                };
                            }
                            Request::CasbinMatchPersonIsAdmin(person_id) => {
                                info!("CasbinMatchPersonIsAdmin({:?})", person_id);

                                response = match match_person_is_admin(&db_connection, person_id) {
                                    Ok(o) => Ok(Box::new(o)),
                                    Err(e) => Err(e.to_string()),
                                };
                            }
                            Request::CasbinMatchPersonIsManager(person_id) => {
                                info!("CasbinMatchPersonIsManager({:?})", person_id);

                                response = match match_person_is_manager(&db_connection, person_id)
                                {
                                    Ok(o) => Ok(Box::new(o)),
                                    Err(e) => Err(e.to_string()),
                                };
                            }
                            Request::CasbinMatchEntityHasMembers(entity_id) => {
                                info!("CasbinMatchEntityHasMembers({:?})", entity_id);

                                response = match match_entity_has_members(&db_connection, entity_id)
                                {
                                    Ok(o) => Ok(Box::new(o)),
                                    Err(e) => Err(e.to_string()),
                                };
                            }
                            Request::CasbinMatchEntityHasStoreLocations(entity_id) => {
                                info!("CasbinMatchEntityHasStoreLocations({:?})", entity_id);

                                response = match match_entity_has_store_locations(
                                    &db_connection,
                                    entity_id,
                                ) {
                                    Ok(o) => Ok(Box::new(o)),
                                    Err(e) => Err(e.to_string()),
                                };
                            }
                            Request::DBDeleteEntity(entity_id) => {
                                info!("DBDeleteEntity({:?})", entity_id);

                                response = match delete_entity(&mut db_connection, entity_id) {
                                    Ok(()) => Ok(Box::new(())),
                                    Err(e) => Err(e.to_string()),
                                };
                            }
                            Request::CasbinMatchStorageIsInEntity(storage_id, entity_id) => {
                                info!(
                                    "CasbinMatchStorageIsInEntity({:?},{:?})",
                                    storage_id, entity_id
                                );

                                response = match match_storage_is_in_entity(
                                    &db_connection,
                                    storage_id,
                                    entity_id,
                                ) {
                                    Ok(o) => Ok(Box::new(o)),
                                    Err(e) => Err(e.to_string()),
                                };
                            }
                            Request::CasbinMatchStoreLocationIsInEntity(
                                store_location_id,
                                entity_id,
                            ) => {
                                info!(
                                    "CasbinMatchStoreLocationIsInEntity({:?},{:?})",
                                    store_location_id, entity_id
                                );

                                response = match match_store_location_is_in_entity(
                                    &db_connection,
                                    store_location_id,
                                    entity_id,
                                ) {
                                    Ok(o) => Ok(Box::new(o)),
                                    Err(e) => Err(e.to_string()),
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
